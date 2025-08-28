<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage;

use DateTimeImmutable;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Schema\Name\Identifier;
use Doctrine\DBAL\Schema\Name\UnqualifiedName;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Types;
use LogicException;
use Shredio\KeyLockedStorage\Value\LockedList;
use Shredio\KeyLockedStorage\Value\LockedValue;
use Symfony\Component\Clock\DatePoint;
use Throwable;

final class DoctrineKeyLockedStorage implements KeyLockedStorage
{

	private bool $autoSetup = true;

	public function __construct(
		private readonly Connection $connection,
		private readonly string $tableName = 'key_locked_storage',
	)
	{
	}

	public function value(string $key, callable $initializer, callable $processor): mixed
	{
		return $this->execute($key, LockedValue::class, $initializer, $processor);
	}

	public function list(string $key, callable $initializer, callable $processor): mixed
	{
		return $this->execute($key, LockedList::class, $initializer, $processor);
	}

	public function get(string $key): mixed
	{
		if (strlen($key) > 120) {
			throw new LogicException(sprintf('Key length %d exceeds maximum length of 120 characters', strlen($key)));
		}

		try {
			return $this->getValue($key, false);
		} catch (TableNotFoundException $exception) {
			if ($this->autoSetup) {
				$this->setup();
				return $this->getValue($key, false);
			}

			throw $exception;
		}
	}

	/**
	 * @template TClass of LockedList|LockedValue
	 * @template TRet
	 * @param class-string<TClass> $valueClass
	 * @param callable(TClass $value): TRet $processor
	 * @return TRet
	 */
	private function execute(string $key, string $valueClass, callable $initializer, callable $processor): mixed
	{
		if (strlen($key) > 120) {
			throw new LogicException(sprintf('Key length %d exceeds maximum length of 120 characters', strlen($key)));
		}

		retry:
		$this->connection->beginTransaction();

		try {
			$raw = $this->getValue($key);
			$keyExists = $raw !== null;
			$initialValue = $keyExists ? $raw : $initializer();

			$return = $processor($value = $valueClass::createFromDatabase($initialValue)); // @phpstan-ignore argument.type
			$snapshot = $value->snapshot();

			if ($snapshot->changed) {
				$this->setValue($key, $snapshot->remove ? null : $snapshot->value, $keyExists);
			}

			$this->connection->commit();

			return $return;
		} catch (Throwable $exception) {
			$this->connection->rollBack();

			if ($this->autoSetup && $exception instanceof TableNotFoundException) {
				$this->setup();
				goto retry;
			}

			throw $exception;
		}
	}

	private function getValue(string $key, bool $withLock = true): mixed
	{
		$qb = $this->connection->createQueryBuilder()
			->select('s.val')
			->from($this->tableName, 's')
			->where('s.id = ?')
			->setParameters([$key], [Types::STRING]);
		
		if ($withLock && !$this->connection->getDatabasePlatform() instanceof SQLitePlatform) {
			$qb->forUpdate();
		}

		$results = $qb->fetchAllAssociative();
		$currentValue = $results[0]['val'] ?? null;
		
		return is_string($currentValue) ? json_decode($currentValue, true, JSON_THROW_ON_ERROR) : null;
	}

	private function setValue(string $key, mixed $value, bool $exists): void
	{
		$qb = $this->connection->createQueryBuilder();

		if ($value === null) {
			if ($exists) {
				$qb->delete($this->tableName)
					->where('id = ?')
					->setParameters([$key], [Types::STRING])
					->executeStatement();
			}

			return;
		}

		$now = class_exists(DatePoint::class) ? new DatePoint() : new DateTimeImmutable();
		
		if ($exists) {
			$qb->update($this->tableName)
				->set('val', '?')
				->set('updated_at', '?')
				->where('id = ?')
				->setParameters([
					json_encode($value, JSON_THROW_ON_ERROR),
					$now,
					$key,
				], [
					Types::STRING,
					Types::DATETIME_IMMUTABLE,
					Types::STRING,
				])
				->executeStatement();
		} else {
			$qb->insert($this->tableName)
				->values([
					'id' => '?',
					'val' => '?',
					'updated_at' => '?',
				])
				->setParameters([
					$key,
					json_encode($value, JSON_THROW_ON_ERROR),
					$now,
				], [
					Types::STRING,
					Types::STRING,
					Types::DATETIME_IMMUTABLE,
				])
				->executeStatement();
		}
	}

	private function setup(): void
	{
		$schema = new Schema();
		$table = $schema->createTable($this->tableName);
		$table->addColumn('id', Types::STRING)
			->setLength(120)
			->setNotnull(true);
		$table->addColumn('val', Types::JSON)
			->setNotnull(false);
		$table->addColumn('updated_at', Types::DATETIME_IMMUTABLE)
			->setNotnull(true);

		if (method_exists($table, 'addPrimaryKeyConstraint')) { // @phpstan-ignore function.alreadyNarrowedType
			$table->addPrimaryKeyConstraint(
				new PrimaryKeyConstraint(null, [new UnqualifiedName(Identifier::unquoted('id'))], false)
			);
		} else {
			$table->setPrimaryKey(['id']);
		}

		$platform = $this->connection->getDatabasePlatform();

		foreach ($platform->getCreateTablesSQL([$table]) as $sql) {
			$this->connection->executeStatement($sql);
		}

		$this->autoSetup = false;
	}

}
