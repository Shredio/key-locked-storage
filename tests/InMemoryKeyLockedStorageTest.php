<?php declare(strict_types = 1);

namespace Tests;

use LogicException;
use RuntimeException;
use Shredio\KeyLockedStorage\InMemoryKeyLockedStorage;

final class InMemoryKeyLockedStorageTest extends TestCase
{
	private InMemoryKeyLockedStorage $storage;

	protected function setUp(): void
	{
		parent::setUp();

		$this->storage = new InMemoryKeyLockedStorage();
	}

	public function testProcessWithNewKey(): void
	{
		$result = $this->storage->run('test-key', function ($value) {
			$this->assertNull($value);
			return ['counter' => 1];
		});

		$this->assertSame(['counter' => 1], $result);
	}

	public function testProcessWithExistingKey(): void
	{
		// First process to create initial value
		$this->storage->run('test-key', function ($value) {
			return ['counter' => 1];
		});

		// Second process to modify existing value
		$result = $this->storage->run('test-key', function ($value) {
			$this->assertSame(['counter' => 1], $value);
			return ['counter' => $value['counter'] + 1];
		});

		$this->assertSame(['counter' => 2], $result);
	}

	public function testProcessWithMultipleKeys(): void
	{
		$result1 = $this->storage->run('key1', function ($value) {
			return ['value' => 'A'];
		});

		$result2 = $this->storage->run('key2', function ($value) {
			return ['value' => 'B'];
		});

		$this->assertSame(['value' => 'A'], $result1);
		$this->assertSame(['value' => 'B'], $result2);

		// Verify each key maintains its own value
		$result1Again = $this->storage->run('key1', function ($value) {
			$this->assertSame(['value' => 'A'], $value);
			return $value;
		});

		$this->assertSame(['value' => 'A'], $result1Again);
	}

	public function testProcessWithCallbackException(): void
	{
		$this->expectException(RuntimeException::class);
		$this->expectExceptionMessage('Test exception');

		$this->storage->run('test-key', function ($value) {
			throw new RuntimeException('Test exception');
		});
	}

	public function testProcessWithJsonSerializableData(): void
	{
		$data = [
			'string' => 'hello',
			'number' => 42,
			'array' => [1, 2, 3],
			'object' => ['nested' => 'value']
		];

		$result = $this->storage->run('json-test', function ($value) use ($data) {
			return $data;
		});

		$this->assertSame($data, $result);

		// Verify it persists correctly
		$result2 = $this->storage->run('json-test', function ($value) use ($data) {
			$this->assertSame($data, $value);
			return $value;
		});

		$this->assertSame($data, $result2);
	}

	public function testKeyLengthValidation(): void
	{
		$longKey = str_repeat('a', 121);
		
		$this->expectException(LogicException::class);
		$this->expectExceptionMessage('Key length 121 exceeds maximum length of 120 characters');

		$this->storage->run($longKey, function ($value) {
			return 'test';
		});
	}

	public function testMaxKeyLengthAllowed(): void
	{
		$maxKey = str_repeat('a', 120);
		
		$result = $this->storage->run($maxKey, function ($value) {
			return 'test';
		});

		$this->assertSame('test', $result);
	}

	public function testPushWithNewKey(): void
	{
		$result = $this->storage->push('array-key', 'first', 'second');

		$this->assertSame(['first', 'second'], $result);
	}

	public function testPushWithExistingArray(): void
	{
		$this->storage->push('array-key', 'first');
		$result = $this->storage->push('array-key', 'second', 'third');

		$this->assertSame(['first', 'second', 'third'], $result);
	}

	public function testPushWithExistingNonArray(): void
	{
		$this->storage->run('non-array-key', fn() => 'string-value');
		$result = $this->storage->push('non-array-key', 'first', 'second');

		$this->assertSame(['first', 'second'], $result);
	}

	public function testPushMultipleValues(): void
	{
		$result = $this->storage->push('multi-key', 1, 2, 3, 'four', ['nested']);

		$this->assertSame([1, 2, 3, 'four', ['nested']], $result);
	}

	public function testPopFromNewKey(): void
	{
		$result = $this->storage->pop('empty-key');

		$this->assertSame([], $result);
	}

	public function testPopSingleElement(): void
	{
		$this->storage->push('pop-key', 'first', 'second', 'third');
		$result = $this->storage->pop('pop-key');

		$this->assertSame(['third'], $result);

		$remaining = $this->storage->run('pop-key', fn($value) => $value);
		$this->assertSame(['first', 'second'], $remaining);
	}

	public function testPopMultipleElements(): void
	{
		$this->storage->push('pop-multi-key', 'a', 'b', 'c', 'd', 'e');
		$result = $this->storage->pop('pop-multi-key', 3);

		$this->assertSame(['c', 'd', 'e'], $result);

		$remaining = $this->storage->run('pop-multi-key', fn($value) => $value);
		$this->assertSame(['a', 'b'], $remaining);
	}

	public function testPopMoreThanAvailable(): void
	{
		$this->storage->push('pop-limited-key', 'one', 'two');
		$result = $this->storage->pop('pop-limited-key', 5);

		$this->assertSame(['one', 'two'], $result);

		$remaining = $this->storage->run('pop-limited-key', fn($value) => $value);
		$this->assertNull($remaining);
	}

	public function testUnshiftWithNewKey(): void
	{
		$result = $this->storage->unshift('unshift-key', 'first', 'second');

		$this->assertSame(['first', 'second'], $result);
	}

	public function testUnshiftWithExistingArray(): void
	{
		$this->storage->push('unshift-existing-key', 'third', 'fourth');
		$result = $this->storage->unshift('unshift-existing-key', 'first', 'second');

		$this->assertSame(['first', 'second', 'third', 'fourth'], $result);
	}

	public function testUnshiftWithExistingNonArray(): void
	{
		$this->storage->run('unshift-non-array-key', fn() => 42);
		$result = $this->storage->unshift('unshift-non-array-key', 'first', 'second');

		$this->assertSame(['first', 'second'], $result);
	}

	public function testShiftFromNewKey(): void
	{
		$result = $this->storage->shift('shift-empty-key');

		$this->assertSame([], $result);
	}

	public function testShiftSingleElement(): void
	{
		$this->storage->push('shift-key', 'first', 'second', 'third');
		$result = $this->storage->shift('shift-key');

		$this->assertSame(['first'], $result);

		$remaining = $this->storage->run('shift-key', fn($value) => $value);
		$this->assertSame(['second', 'third'], $remaining);
	}

	public function testShiftMultipleElements(): void
	{
		$this->storage->push('shift-multi-key', 'a', 'b', 'c', 'd', 'e');
		$result = $this->storage->shift('shift-multi-key', 3);

		$this->assertSame(['a', 'b', 'c'], $result);

		$remaining = $this->storage->run('shift-multi-key', fn($value) => $value);
		$this->assertSame(['d', 'e'], $remaining);
	}

	public function testShiftMoreThanAvailable(): void
	{
		$this->storage->push('shift-limited-key', 'one', 'two');
		$result = $this->storage->shift('shift-limited-key', 5);

		$this->assertSame(['one', 'two'], $result);

		$remaining = $this->storage->run('shift-limited-key', fn($value) => $value);
		$this->assertNull($remaining);
	}

	public function testArrayOperationsSequence(): void
	{
		$this->storage->push('sequence-key', 'a', 'b');
		$this->assertSame(['a', 'b'], $this->storage->run('sequence-key', fn($value) => $value));

		$this->storage->unshift('sequence-key', 'x', 'y');
		$this->assertSame(['x', 'y', 'a', 'b'], $this->storage->run('sequence-key', fn($value) => $value));

		$popped = $this->storage->pop('sequence-key', 2);
		$this->assertSame(['a', 'b'], $popped);
		$this->assertSame(['x', 'y'], $this->storage->run('sequence-key', fn($value) => $value));

		$shifted = $this->storage->shift('sequence-key');
		$this->assertSame(['x'], $shifted);
		$this->assertSame(['y'], $this->storage->run('sequence-key', fn($value) => $value));
	}

	public function testEmptyArrayCleanup(): void
	{
		$this->storage->push('cleanup-key', 'item');
		$this->assertSame(['item'], $this->storage->run('cleanup-key', fn($value) => $value));

		$this->storage->pop('cleanup-key');
		$this->assertNull($this->storage->run('cleanup-key', fn($value) => $value));

		$this->storage->push('cleanup-key2', 'item');
		$this->storage->shift('cleanup-key2');
		$this->assertNull($this->storage->run('cleanup-key2', fn($value) => $value));
	}

	public function testStorageIsolation(): void
	{
		$storage1 = new InMemoryKeyLockedStorage();
		$storage2 = new InMemoryKeyLockedStorage();

		$storage1->run('shared-key', fn() => 'value1');
		$storage2->run('shared-key', fn() => 'value2');

		$result1 = $storage1->run('shared-key', fn($value) => $value);
		$result2 = $storage2->run('shared-key', fn($value) => $value);

		$this->assertSame('value1', $result1);
		$this->assertSame('value2', $result2);
	}
}