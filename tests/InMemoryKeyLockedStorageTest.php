<?php declare(strict_types = 1);

namespace Tests;

use LogicException;
use RuntimeException;
use Shredio\KeyLockedStorage\InMemoryKeyLockedStorage;
use Shredio\KeyLockedStorage\Value\LockedList;
use Shredio\KeyLockedStorage\Value\LockedValue;

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
		$result = $this->storage->value('test-key', fn() => ['counter' => 1], function ($value) {
			$value->set(['counter' => 1]);
			return $value->get();
		});

		$this->assertSame(['counter' => 1], $result);
	}

	public function testProcessWithExistingKey(): void
	{
		// First process to create initial value
		$this->storage->value('test-key', fn() => ['counter' => 1], function ($value) {
			$value->set(['counter' => 1]);
			return $value->get();
		});

		// Second process to modify existing value
		$result = $this->storage->value('test-key', fn() => ['counter' => 1], function ($value) {
			$this->assertSame(['counter' => 1], $value->get());
			$value->set(['counter' => $value->get()['counter'] + 1]);
			return $value->get();
		});

		$this->assertSame(['counter' => 2], $result);
	}

	public function testProcessWithMultipleKeys(): void
	{
		$result1 = $this->storage->value('key1', fn() => ['value' => 'A'], function ($value) {
			$value->set(['value' => 'A']);
			return $value->get();
		});

		$result2 = $this->storage->value('key2', fn() => ['value' => 'B'], function ($value) {
			$value->set(['value' => 'B']);
			return $value->get();
		});

		$this->assertSame(['value' => 'A'], $result1);
		$this->assertSame(['value' => 'B'], $result2);

		// Verify each key maintains its own value
		$result1Again = $this->storage->value('key1', fn() => ['value' => 'A'], function ($value) {
			$this->assertSame(['value' => 'A'], $value->get());
			return $value->get();
		});

		$this->assertSame(['value' => 'A'], $result1Again);
	}

	public function testProcessWithCallbackException(): void
	{
		$this->expectException(RuntimeException::class);
		$this->expectExceptionMessage('Test exception');

		$this->storage->value('test-key', fn() => ['test' => 'data'], function ($value) {
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

		$result = $this->storage->value('json-test', fn() => $data, function ($value) use ($data) {
			$value->set($data);
			return $value->get();
		});

		$this->assertSame($data, $result);

		// Verify it persists correctly
		$result2 = $this->storage->value('json-test', fn() => $data, function ($value) use ($data) {
			$this->assertSame($data, $value->get());
			return $value->get();
		});

		$this->assertSame($data, $result2);
	}

	public function testKeyLengthValidation(): void
	{
		$longKey = str_repeat('a', 121);
		
		$this->expectException(LogicException::class);
		$this->expectExceptionMessage('Key length 121 exceeds maximum length of 120 characters');

		$this->storage->value($longKey, fn() => 'test', function (LockedValue $value) {
			return $value->get();
		});
	}

	public function testMaxKeyLengthAllowed(): void
	{
		$maxKey = str_repeat('a', 120);
		
		$result = $this->storage->value($maxKey, fn() => 'test', function (LockedValue $value) {
			$value->set('test');
			return $value->get();
		});

		$this->assertSame('test', $result);
	}

	public function testStorageIsolation(): void
	{
		$storage1 = new InMemoryKeyLockedStorage();
		$storage2 = new InMemoryKeyLockedStorage();

		$storage1->value('shared-key', fn() => 'value1', fn(LockedValue $value) => $value->get());
		$storage2->value('shared-key', fn() => 'value2', fn(LockedValue $value) => $value->get());

		$result1 = $storage1->value('shared-key', fn() => 'value1', fn(LockedValue $value) => $value->get());
		$result2 = $storage2->value('shared-key', fn() => 'value2', fn(LockedValue $value) => $value->get());

		$this->assertSame('value1', $result1);
		$this->assertSame('value2', $result2);
	}

	public function testListWithNewKey(): void
	{
		$result = $this->storage->list('list-key', fn() => ['a', 'b', 'c'], function(LockedList $list) {
			$this->assertSame(['a', 'b', 'c'], $list->getValues());
			return $list->getValues();
		});

		$this->assertSame(['a', 'b', 'c'], $result);
	}

	public function testListPushOperation(): void
	{
		$this->storage->list('list-key', fn() => ['a', 'b'], function(LockedList $list) {
			$list->set(['a', 'b']);
			return $list->getValues();
		});

		$result = $this->storage->list('list-key', fn() => [], function(LockedList $list) {
			$this->assertSame(['a', 'b'], $list->getValues());
			$list->push('c', 'd');
			return $list->getValues();
		});

		$this->assertSame(['a', 'b', 'c', 'd'], $result);
		
		$stored = $this->storage->get('list-key');
		$this->assertSame(['a', 'b', 'c', 'd'], $stored);
	}

	public function testListPopOperation(): void
	{
		$this->storage->list('list-key', fn() => ['a', 'b', 'c', 'd'], function(LockedList $list) {
			$list->set(['a', 'b', 'c', 'd']);
			return $list->getValues();
		});

		$result = $this->storage->list('list-key', fn() => [], function(LockedList $list) {
			$popped = $list->pop(2);
			return $popped;
		});

		$this->assertSame(['c', 'd'], $result);
		
		$stored = $this->storage->get('list-key');
		$this->assertSame(['a', 'b'], $stored);
	}

	public function testListUnshiftOperation(): void
	{
		$this->storage->list('list-key', fn() => ['c', 'd'], function(LockedList $list) {
			$list->set(['c', 'd']);
			return $list->getValues();
		});

		$result = $this->storage->list('list-key', fn() => [], function(LockedList $list) {
			$list->unshift('a', 'b');
			return $list->getValues();
		});

		$this->assertSame(['a', 'b', 'c', 'd'], $result);
		
		$stored = $this->storage->get('list-key');
		$this->assertSame(['a', 'b', 'c', 'd'], $stored);
	}

	public function testListShiftOperation(): void
	{
		$this->storage->list('list-key', fn() => ['a', 'b', 'c', 'd'], function(LockedList $list) {
			$list->set(['a', 'b', 'c', 'd']);
			return $list->getValues();
		});

		$result = $this->storage->list('list-key', fn() => [], function(LockedList $list) {
			$shifted = $list->shift(2);
			return $shifted;
		});

		$this->assertSame(['a', 'b'], $result);
		
		$stored = $this->storage->get('list-key');
		$this->assertSame(['c', 'd'], $stored);
	}

	public function testListRemoveOperation(): void
	{
		$this->storage->list('list-key', fn() => ['a', 'b', 'c'], function(LockedList $list) {
			$list->set(['a', 'b', 'c']);
			return $list->getValues();
		});

		$this->storage->list('list-key', fn() => [], function(LockedList $list) {
			$list->remove();
			return $list->getValues();
		});
		
		$stored = $this->storage->get('list-key');
		$this->assertNull($stored);
	}

	public function testListWithEmptyInitializer(): void
	{
		$result = $this->storage->list('empty-list-key', fn() => [], function(LockedList $list) {
			$this->assertSame([], $list->getValues());
			$list->push('first');
			return $list->getValues();
		});

		$this->assertSame(['first'], $result);
		
		$stored = $this->storage->get('empty-list-key');
		$this->assertSame(['first'], $stored);
	}

	public function testListKeyLengthValidation(): void
	{
		$longKey = str_repeat('a', 121);
		
		$this->expectException(LogicException::class);
		$this->expectExceptionMessage('Key length 121 exceeds maximum length of 120 characters');

		$this->storage->list($longKey, fn() => ['test'], function(LockedList $list) {
			return $list->getValues();
		});
	}

	public function testValueRemoveOperation(): void
	{
		$this->storage->value('remove-key', fn() => ['data' => 'test'], function(LockedValue $value) {
			$value->set(['data' => 'test']);
			return $value->get();
		});

		$this->storage->value('remove-key', fn() => ['data' => 'test'], function(LockedValue $value) {
			$this->assertSame(['data' => 'test'], $value->get());
			$value->remove();
			return null;
		});
		
		$stored = $this->storage->get('remove-key');
		$this->assertNull($stored);
	}

	public function testValueRollbackOperation(): void
	{
		// First establish original data in storage
		$this->storage->value('rollback-key', fn() => ['initial' => 'data'], function(LockedValue $value) {
			$value->set(['original' => 'data']);
			return $value->get();
		});

		// Now test rollback behavior
		$result = $this->storage->value('rollback-key', fn() => null, function(LockedValue $value) {
			// Value should be loaded from storage
			$this->assertSame(['original' => 'data'], $value->get());
			
			// Modify the value
			$value->set(['modified' => 'data']);
			$this->assertSame(['modified' => 'data'], $value->get());
			
			// Rollback to original state
			$value->rollback();
			$this->assertSame(['original' => 'data'], $value->get());
			
			return $value->get();
		});
		
		$this->assertSame(['original' => 'data'], $result);
		
		// Verify rollback prevented changes from being saved - original data should still be there
		$stored = $this->storage->get('rollback-key');
		$this->assertSame(['original' => 'data'], $stored);
	}

	public function testListRollbackOperation(): void
	{
		$this->storage->list('rollback-list-key', fn() => ['a', 'b', 'c'], function(LockedList $list) {
			$list->set(['a', 'b', 'c']);
			return $list->getValues();
		});

		$result = $this->storage->list('rollback-list-key', fn() => ['a', 'b', 'c'], function(LockedList $list) {
			$this->assertSame(['a', 'b', 'c'], $list->getValues());
			
			$list->push('d', 'e');
			$this->assertSame(['a', 'b', 'c', 'd', 'e'], $list->getValues());
			
			$list->pop(1);
			$this->assertSame(['a', 'b', 'c', 'd'], $list->getValues());
			
			$list->rollback();
			$this->assertSame(['a', 'b', 'c'], $list->getValues());
			
			return $list->getValues();
		});
		
		$this->assertSame(['a', 'b', 'c'], $result);
		
		// Verify rollback prevented changes from being saved
		$stored = $this->storage->get('rollback-list-key');
		$this->assertSame(['a', 'b', 'c'], $stored);
	}
}
