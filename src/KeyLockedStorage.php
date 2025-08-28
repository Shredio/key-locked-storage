<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage;

interface KeyLockedStorage
{

	/**
	 * @template T
	 * @param callable(T|null): T $callback
	 * @return T
	 */
	public function run(string $key, callable $callback): mixed;

	/**
	 * Returns the value for the given key (without a lock)
	 */
	public function get(string $key): mixed;

	/**
	 * Adds one or more elements to the end of an array
	 * @param list<mixed> $values
	 * @return list<mixed>
	 */
	public function push(string $key, mixed ...$values): array;

	/**
	 * Removes and returns elements from the end of an array
	 * @param int<1, max> $count
	 * @return list<mixed>
	 */
	public function pop(string $key, int $count = 1): array;

	/**
	 * Adds one or more elements to the beginning of an array
	 * @param list<mixed> $values
	 * @return list<mixed>
	 */
	public function unshift(string $key, mixed ...$values): array;

	/**
	 * Removes and returns elements from the beginning of an array
	 * @param int<1, max> $count
	 * @return list<mixed>
	 */
	public function shift(string $key, int $count = 1): array;

	/**
	 * Removes and returns elements from the end of an array, initializing with callback if empty
	 * @template T
	 * @param callable(): list<T> $initializer
	 * @param int<1, max> $count
	 * @return list<T>
	 */
	public function popOrInit(string $key, callable $initializer, int $count = 1): array;

	/**
	 * Removes and returns elements from the beginning of an array, initializing with callback if empty
	 * @template T
	 * @param callable(): list<T> $initializer
	 * @param int<1, max> $count
	 * @return list<T>
	 */
	public function shiftOrInit(string $key, callable $initializer, int $count = 1): array;

}
