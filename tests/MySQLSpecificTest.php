<?php declare(strict_types = 1);

namespace Tests;

use Doctrine\DBAL\Connection;
use Shredio\KeyLockedStorage\DoctrineKeyLockedStorage;

final class MySQLSpecificTest extends TestCase
{
	private Connection $connection;
	
	protected function setUp(): void
	{
		parent::setUp();
		
		if (!DatabaseConnectionFactory::isMySQL()) {
			$this->markTestSkipped('This test requires MySQL');
		}
		
		$this->connection = DatabaseConnectionFactory::createMySQL();
	}

	public function testAutoSetupCreatesMultipleTables(): void
	{
		// Create articles table with basic structure
		$this->connection->executeStatement('
			CREATE TABLE IF NOT EXISTS articles (
				id INT PRIMARY KEY AUTO_INCREMENT,
				subject VARCHAR(255) NOT NULL,
				content TEXT
			)
		');

		// Create storage for default table
		$storage = new DoctrineKeyLockedStorage($this->connection, 'key_locked_storage');
		$storage->run('some-key', function ($value) {
			return ['data' => 'test'];
		});
		
		// Verify both tables exist
		$schemaManager = $this->connection->createSchemaManager();
		$tableNames = $schemaManager->listTableNames();
		
		$this->assertContains('articles', $tableNames);
		$this->assertContains('key_locked_storage', $tableNames);
		
		// Verify articles table still has original structure
		$articlesTable = $schemaManager->introspectTable('articles');
		$this->assertTrue($articlesTable->hasColumn('id'));
		$this->assertTrue($articlesTable->hasColumn('subject'));
		$this->assertTrue($articlesTable->hasColumn('content'));
		
		// Clean up
		$this->connection->executeStatement('DROP TABLE IF EXISTS articles');
	}
}
