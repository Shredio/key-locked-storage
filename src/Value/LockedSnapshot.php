<?php declare(strict_types = 1);

namespace Shredio\KeyLockedStorage\Value;

final readonly class LockedSnapshot
{

	public function __construct(
		public bool $changed,
		public bool $remove,
		public mixed $value,
	)
	{
	}

}
