<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage;

use LogicException;
use Shredio\KeyLockedStorage\Value\LockedList;
use Shredio\KeyLockedStorage\Value\LockedValue;

final class InMemoryKeyLockedStorage implements KeyLockedStorage
{

	/**
	 * @var array<string, mixed>
	 */
	private array $storage = [];

	public function value(string $key, callable $initializer, callable $processor): mixed
	{
		return $this->execute($key, LockedValue::class, $initializer, $processor);
	}

	public function list(string $key, callable $initializer, callable $processor): mixed
	{
		return $this->execute($key, LockedList::class, $initializer, $processor);
	}

	public function get(string $key, bool $delete = false): mixed
	{
		if (strlen($key) > 120) {
			throw new LogicException(sprintf('Key length %d exceeds maximum length of 120 characters', strlen($key)));
		}

		$value = $this->storage[$key] ?? null;
		if ($delete) {
			unset($this->storage[$key]);
		}

		return $value;
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

		$raw = $this->storage[$key] ?? null;
		$keyExists = $raw !== null;
		$initialValue = $keyExists ? $raw : $initializer();

		$return = $processor($value = $valueClass::createFromDatabase($initialValue)); // @phpstan-ignore argument.type
		$snapshot = $value->snapshot();

		if ($snapshot->changed) {
			 if ($snapshot->remove) {
				unset($this->storage[$key]);
			} else {
				$this->storage[$key] = $snapshot->value;
			}
		}

		return $return;
	}


}
