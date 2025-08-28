<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage\Value;

/**
 * @template T
 */
final class LockedList
{

	/** @var list<T> */
	private array $originalValues;

	/** @var list<T> */
	private array $values;

	private bool $changed = false;

	private bool $remove = false;

	/**
	 * @param list<T> $values
	 */
	public function __construct(array $values = [])
	{
		$this->originalValues = $this->values = $values;
	}

	/**
	 * @param list<T> $values
	 */
	public function set(array $values): void
	{
		$this->changed = true;

		$this->values = $values;
	}

	/**
	 * Adds one or more elements to the end of the list
	 * @param T ...$values
	 */
	public function push(mixed ...$values): void
	{
		$this->changed = true;

		$this->values = array_values([...$this->values, ...$values]);
	}

	/**
	 * Removes and returns elements from the end of the list
	 * @param int<1, max> $count
	 * @return list<T>
	 */
	public function pop(int $count = 1): array
	{
		$this->changed = true;

		return array_splice($this->values, -$count);
	}

	/**
	 * Adds one or more elements to the beginning of the list
	 * @param T ...$values
	 */
	public function unshift(mixed ...$values): void
	{
		$this->changed = true;

		$this->values = array_values([...$values, ...$this->values]);
	}

	/**
	 * Removes and returns elements from the beginning of the list
	 * @param int<1, max> $count
	 * @return list<T>
	 */
	public function shift(int $count = 1): array
	{
		$this->changed = true;

		return array_splice($this->values, 0, $count);
	}

	/**
	 * @return list<T>
	 */
	public function getValues(): array
	{
		return $this->values;
	}

	public function remove(): void
	{
		$this->changed = true;
		$this->remove = true;
	}

	public function rollback(): void
	{
		$this->changed = false;
		$this->remove = false;
		$this->values = $this->originalValues;
	}

	/**
	 * @internal
	 */
	public function snapshot(): LockedSnapshot
	{
		return new LockedSnapshot($this->changed, $this->remove, $this->values);
	}

	/**
	 * @return self<mixed>
	 */
	public static function createFromDatabase(mixed $value): self
	{
		/** @var list<mixed> $list */
		$list = is_array($value) ? $value : [];

		return new self($list);
	}

}
