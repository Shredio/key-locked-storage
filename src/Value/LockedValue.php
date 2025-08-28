<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage\Value;

/**
 * @template T
 */
final class LockedValue
{

	/** @var T */
	private mixed $originalValue;

	/** @var T */
	private mixed $value;

	private bool $changed = false;

	private bool $remove = false;

	/**
	 * @param T $value
	 */
	public function __construct(mixed $value)
	{
		$this->originalValue = $this->value = $value;
	}

	/**
	 * @param T $value
	 */
	public function set(mixed $value): void
	{
		$this->value = $value;
		$this->changed = $this->originalValue !== $value;
	}

	public function get(): mixed
	{
		return $this->value;
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
		$this->value = $this->originalValue;
	}

	/**
	 * @internal
	 */
	public function snapshot(): LockedSnapshot
	{
		return new LockedSnapshot($this->changed, $this->remove, $this->value);
	}

	/**
	 * @return self<mixed>
	 */
	public static function createFromDatabase(mixed $value): self
	{
		return new self($value);
	}

}
