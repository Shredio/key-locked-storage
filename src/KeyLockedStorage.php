<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage;

use Shredio\KeyLockedStorage\Value\LockedList;
use Shredio\KeyLockedStorage\Value\LockedValue;

interface KeyLockedStorage
{

	/**
	 * @template T
	 * @template TRet
	 * @param callable(): T $initializer
	 * @param callable(LockedValue<T> $value): TRet $processor
	 * @return TRet
	 */
	public function value(string $key, callable $initializer, callable $processor): mixed;

	/**
	 * @template T
	 * @template TRet
	 * @param callable(): list<T> $initializer
	 * @param callable(LockedList<T> $list): TRet $processor
	 * @return TRet
	 */
	public function list(string $key, callable $initializer, callable $processor): mixed;

	/**
	 * Returns the value for the given key (without a lock)
	 *
	 * @param bool $delete Whether to delete the value after fetching it - if true deleted with lock
	 */
	public function get(string $key, bool $delete = false): mixed;

}
