<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Query\ForUpdate\ConflictResolutionMode;
use Doctrine\DBAL\Schema\Name\Identifier;
use Doctrine\DBAL\Schema\Name\UnqualifiedName;
use Doctrine\DBAL\Schema\PrimaryKeyConstraint;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Types\Types;
use LogicException;
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

	public function run(string $key, callable $callback): mixed
	{
		return $this->execute($key, static function (mixed $current) use ($callback): KeyLockedValue {
			return KeyLockedValue::single($callback($current));
		});
	}

	/**
	 * @template TValue
	 * @param callable(TValue|null $value): KeyLockedValue<TValue> $callback
	 * @return TValue
	 */
	private function execute(string $key, callable $callback): mixed
	{
		if (strlen($key) > 120) {
			throw new LogicException(sprintf('Key length %d exceeds maximum length of 120 characters', strlen($key)));
		}

		retry:
		$this->connection->beginTransaction();

		try {
			$currentValue = $this->getValue($key);
			$ret = $callback($currentValue);
			$this->setValue($key, $ret->valueToSet, $currentValue !== null);
			$this->connection->commit();

			return $ret->valueToReturn;
		} catch (Throwable $exception) {
			$this->connection->rollBack();

			if ($this->autoSetup && $exception instanceof TableNotFoundException) {
				$this->setup();
				goto retry;
			}

			throw $exception;
		}
	}

	private function getValue(string $key): mixed
	{
		$qb = $this->connection->createQueryBuilder()
			->select('s.val')
			->from($this->tableName, 's')
			->where('s.id = ?')
			->setParameters([$key], [Types::STRING]);
		
		if (!$this->connection->getDatabasePlatform() instanceof SQLitePlatform) {
			$qb->forUpdate(ConflictResolutionMode::SKIP_LOCKED);
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

		if ($exists) {
			$qb->update($this->tableName)
				->set('val', '?')
				->where('id = ?')
				->setParameters([
					json_encode($value, JSON_THROW_ON_ERROR),
					$key,
				], [
					Types::STRING,
					Types::STRING,
				])
				->executeStatement();
		} else {
			$qb->insert($this->tableName)
				->values([
					'id' => '?',
					'val' => '?',
				])
				->setParameters([
					$key,
					json_encode($value, JSON_THROW_ON_ERROR),
				], [
					Types::STRING,
					Types::STRING,
				])
				->executeStatement();
		}
	}

	/**
	 * @return list<mixed>
	 */
	private function ensureList(mixed $value): array
	{
		if (!is_array($value) || !isset($value[0])) {
			return [];
		}

		/** @var list<mixed> */
		return $value;
	}

	public function push(string $key, mixed ...$values): array
	{
		return $this->execute($key, function (mixed $current) use ($values): KeyLockedValue {
			$array = $this->ensureList($current);
			array_push($array, ...$values);

			return KeyLockedValue::single($array);
		});
	}

	public function pop(string $key, int $count = 1): array
	{
		return $this->execute($key, function (mixed $current) use ($count): KeyLockedValue {
			$array = $this->ensureList($current);
			$popped = array_splice($array, -$count);

			return new KeyLockedValue(
				valueToSet: $array === [] ? null : $array,
				valueToReturn: $popped,
			);
		});
	}

	public function unshift(string $key, mixed ...$values): array
	{
		return $this->execute($key, function (mixed $current) use ($values): KeyLockedValue {
			$array = $this->ensureList($current);
			array_unshift($array, ...$values);

			return KeyLockedValue::single($array);
		});
	}

	public function shift(string $key, int $count = 1): array
	{
		return $this->execute($key, function (mixed $current) use (&$shifted, $count): KeyLockedValue {
			$array = $this->ensureList($current);
			$shifted = array_splice($array, 0, $count);

			return new KeyLockedValue(
				valueToSet: $array === [] ? null : $array,
				valueToReturn: $shifted,
			);
		});
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
		$table->addPrimaryKeyConstraint(new PrimaryKeyConstraint(null, [new UnqualifiedName(Identifier::unquoted('id'))], false));

		$platform = $this->connection->getDatabasePlatform();

		foreach ($platform->getCreateTablesSQL([$table]) as $sql) {
			$this->connection->executeStatement($sql);
		}

		$this->autoSetup = false;
	}

	public function popOrInit(string $key, callable $initializer, int $count = 1): array
	{
		return $this->execute($key, function (mixed $current) use ($initializer, $count): KeyLockedValue {
			$array = $this->ensureList($current);
			
			if ($array === []) {
				$array = $initializer();
			}
			
			$popped = array_splice($array, -$count);

			return new KeyLockedValue(
				valueToSet: $array === [] ? null : $array,
				valueToReturn: $popped,
			);
		});
	}

	public function shiftOrInit(string $key, callable $initializer, int $count = 1): array
	{
		return $this->execute($key, function (mixed $current) use ($initializer, $count): KeyLockedValue {
			$array = $this->ensureList($current);
			
			if ($array === []) {
				$array = $initializer();
			}
			
			$shifted = array_splice($array, 0, $count);

			return new KeyLockedValue(
				valueToSet: $array === [] ? null : $array,
				valueToReturn: $shifted,
			);
		});
	}

}
