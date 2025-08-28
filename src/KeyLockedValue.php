<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage;

/**
 * @template T
 * @internal
 */
final readonly class KeyLockedValue
{

	/**
	 * @param mixed $valueToSet
	 * @param T $valueToReturn
	 */
	public function __construct(
		public mixed $valueToSet,
		public mixed $valueToReturn,
	)
	{
	}

	/**
	 * @template TVal
	 * @param TVal $value
	 * @return KeyLockedValue<TVal>
	 */
	public static function single(mixed $value): self
	{
		return new self($value, $value);
	}

	/**
	 * @return KeyLockedValue<null>
	 */
	public static function remove(): self
	{
		return new self(null, null);
	}

}
