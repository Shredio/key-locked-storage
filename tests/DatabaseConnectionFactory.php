<?php declare(strict_types = 1);

namespace Tests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use InvalidArgumentException;

final class DatabaseConnectionFactory
{
	public static function create(): Connection
	{
		$driver = $_ENV['DB_DRIVER'] ?? 'pdo_sqlite';

		if ($driver === 'pdo_sqlite') {
			return self::createSQLite();
		}

		if ($driver === 'pdo_mysql') {
			return self::createMySQL();
		}

		throw new InvalidArgumentException(sprintf('Unsupported driver: %s', $driver));
	}

	public static function createMySQL(): Connection
	{
		return DriverManager::getConnection([
			'driver' => 'pdo_mysql',
			'host' => $_ENV['DB_HOST'] ?? '127.0.0.1',
			'port' => (int) ($_ENV['DB_PORT'] ?? 3306),
			'dbname' => $_ENV['DB_DATABASE'] ?? 'test',
			'user' => $_ENV['DB_USERNAME'] ?? 'root',
			'password' => $_ENV['DB_PASSWORD'] ?? 'root',
		]);
	}

	public static function createSQLite(): Connection
	{
		return DriverManager::getConnection([
			'driver' => 'pdo_sqlite',
			'memory' => true,
		]);
	}

	public static function isMySQL(): bool
	{
		return ($_ENV['DB_DRIVER'] ?? 'pdo_sqlite') === 'pdo_mysql';
	}

	public static function isSQLite(): bool
	{
		return ($_ENV['DB_DRIVER'] ?? 'pdo_sqlite') === 'pdo_sqlite';
	}

	public static function getCurrentDriver(): string
	{
		return $_ENV['DB_DRIVER'] ?? 'pdo_sqlite';
	}
}