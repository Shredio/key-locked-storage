<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage;

use LogicException;

final class InMemoryKeyLockedStorage implements KeyLockedStorage
{

	/**
	 * @var array<string, mixed>
	 */
	private array $storage = [];

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

		$currentValue = $this->storage[$key] ?? null;
		$ret = $callback($currentValue);
		
		if ($ret->valueToSet === null) {
			unset($this->storage[$key]);
		} else {
			$this->storage[$key] = $ret->valueToSet;
		}

		return $ret->valueToReturn;
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
		return $this->execute($key, function (mixed $current) use ($count): KeyLockedValue {
			$array = $this->ensureList($current);
			$shifted = array_splice($array, 0, $count);

			return new KeyLockedValue(
				valueToSet: $array === [] ? null : $array,
				valueToReturn: $shifted,
			);
		});
	}

}