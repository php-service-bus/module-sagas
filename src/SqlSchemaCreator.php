<?php

/**
 * Saga pattern implementation module.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module;

use function Amp\call;
use Amp\Promise;
use ServiceBus\Storage\Common\DatabaseAdapter;

/**
 * @codeCoverageIgnore
 */
final class SqlSchemaCreator
{
    private const FIXTURES = [
        '/src/Store/Sql/schema/extensions.sql'  => false,
        '/src/Store/Sql/schema/sagas_store.sql' => false,
        '/src/Store/Sql/schema/indexes.sql'     => true,
    ];

    /**
     * @var DatabaseAdapter
     */
    private $adapter;

    /**
     * @var string
     */
    private $rootDirectoryPath;

    /**
     * @param DatabaseAdapter $adapter
     * @param string          $rootDirectoryPath
     */
    public function __construct(DatabaseAdapter $adapter, string $rootDirectoryPath)
    {
        $this->adapter           = $adapter;
        $this->rootDirectoryPath = \rtrim($rootDirectoryPath, '/');
    }

    /**
     * Import fixtures.
     *
     * @return Promise
     */
    public function import(): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(array $fixtures): \Generator
            {
                /**
                 * @var string $filePath
                 * @var bool   $multipleQueries
                 */
                foreach ($fixtures as $filePath => $multipleQueries)
                {
                    $filePath = $this->rootDirectoryPath . $filePath;

                    /** @psalm-suppress InvalidScalarArgument */
                    $queries = true === $multipleQueries
                        ? \array_map('trim', (array) \file($filePath))
                        : [(string) \file_get_contents($filePath)];

                    foreach ($queries as $query)
                    {
                        if ('' !== $query)
                        {
                            /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
                            yield $this->adapter->execute($query);
                        }
                    }
                }
            },
            self::FIXTURES
        );
    }
}
