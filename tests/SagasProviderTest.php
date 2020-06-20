<?php

/**
 * Saga pattern implementation module.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module\Tests;

use Amp\Loop;
use function Amp\Promise\wait;
use function ServiceBus\Common\writeReflectionPropertyValue;
use PHPUnit\Framework\TestCase;
use ServiceBus\MessagesRouter\Router;
use ServiceBus\MessagesRouter\Tests\stubs\TestCommand;
use ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga;
use ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound;
use ServiceBus\Sagas\Module\SagaModule;
use ServiceBus\Sagas\Module\SagasProvider;
use ServiceBus\Sagas\Module\Tests\stubs\Context;
use ServiceBus\Sagas\Module\Tests\stubs\TestSaga;
use ServiceBus\Sagas\Module\Tests\stubs\TestSagaId;
use ServiceBus\Sagas\Saga;
use ServiceBus\Sagas\Store\Exceptions\DuplicateSaga;
use ServiceBus\Sagas\Store\Exceptions\LoadedExpiredSaga;
use ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Common\StorageConfiguration;
use ServiceBus\Storage\Sql\DoctrineDBAL\DoctrineDBALAdapter;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use function ServiceBus\Storage\Sql\equalsCriteria;
use function ServiceBus\Storage\Sql\updateQuery;

/**
 *
 */
final class SagasProviderTest extends TestCase
{
    /** @var ContainerBuilder */
    private $containerBuilder;

    /** @var DatabaseAdapter */
    private $adapter;

    /** @var SagasProvider */
    private $sagaProvider;

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->containerBuilder = new ContainerBuilder();

        $this->containerBuilder->addDefinitions([
            StorageConfiguration::class => new Definition(StorageConfiguration::class, ['sqlite:///:memory:']),
            DatabaseAdapter::class      => (new Definition(DoctrineDBALAdapter::class))
                ->setArguments([new Reference(StorageConfiguration::class)]),
        ]);

        SagaModule::withSqlStorage(DatabaseAdapter::class)
            ->enableAutoImportSagas([__DIR__ . '/stubs'])
            ->boot($this->containerBuilder);

        $this->containerBuilder->getDefinition(SagasProvider::class)->setPublic(true);
        $this->containerBuilder->getDefinition(DatabaseAdapter::class)->setPublic(true);
        $this->containerBuilder->getDefinition(Router::class)->setPublic(true);

        $this->containerBuilder->compile();

        $this->adapter      = $this->containerBuilder->get(DatabaseAdapter::class);
        $this->sagaProvider = $this->containerBuilder->get(SagasProvider::class);

        wait(
            $this->adapter->execute(
                \file_get_contents(
                    __DIR__ . '/../vendor/php-service-bus/sagas/src/Store/Sql/schema/sagas_store.sql'
                )
            )
        );

        $indexQueries = \file(__DIR__ . '/../vendor/php-service-bus/sagas/src/Store/Sql/schema/indexes.sql');

        foreach ($indexQueries as $query)
        {
            wait($this->adapter->execute($query));
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        unset($this->containerBuilder, $this->adapter, $this->sagaProvider);
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function updateNonexistentSaga(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->expectException(CantSaveUnStartedSaga::class);

                $testSaga = new TestSaga(TestSagaId::new(TestSaga::class));

                yield $this->sagaProvider->save($testSaga, new Context());
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function startWithoutMetadata(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->expectException(SagaMetaDataNotFound::class);

                $id = TestSagaId::new(TestSaga::class);

                yield $this->sagaProvider->start($id, new TestCommand(), new Context());
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function start(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->containerBuilder->get(Router::class);

                $id = TestSagaId::new(TestSaga::class);

                /** @var Saga $saga */
                $saga = yield $this->sagaProvider->start($id, new TestCommand(), new Context());

                static::assertNotNull($saga);
                static::assertInstanceOf(TestSaga::class, $saga);
                static::assertSame($id, $saga->id());
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function startDuplicate(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->expectException(DuplicateSaga::class);

                $this->containerBuilder->get(Router::class);

                $id = TestSagaId::new(TestSaga::class);

                yield $this->sagaProvider->start($id, new TestCommand(), new Context());
                yield $this->sagaProvider->start($id, new TestCommand(), new Context());
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function startWithoutSchema(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->expectException(SagasStoreInteractionFailed::class);

                yield $this->adapter->execute('DROP TABLE sagas_store');

                $this->containerBuilder->get(Router::class);

                yield $this->sagaProvider->start(TestSagaId::new(TestSaga::class), new TestCommand(), new Context());
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function obtainWithoutSchema(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->expectException(SagasStoreInteractionFailed::class);

                $this->containerBuilder->get(Router::class);

                yield $this->adapter->execute('DROP TABLE sagas_store');
                yield $this->sagaProvider->obtain(TestSagaId::new(TestSaga::class), new Context());
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function saveWithoutSchema(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->expectException(SagasStoreInteractionFailed::class);

                yield $this->adapter->execute('DROP TABLE sagas_store');

                $testSaga = new TestSaga(TestSagaId::new(TestSaga::class));

                yield $this->sagaProvider->save($testSaga, new Context());
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function obtainNonexistentSaga(): void
    {
        Loop::run(
            function (): \Generator
            {
                static::assertNull(
                    yield $this->sagaProvider->obtain(TestSagaId::new(TestSaga::class), new Context())
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function obtainExpiredSaga(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->expectException(LoadedExpiredSaga::class);

                $this->containerBuilder->get(Router::class);

                $context = new Context();
                $id      = TestSagaId::new(TestSaga::class);

                /** @var Saga $saga */
                $saga = yield $this->sagaProvider->start($id, new TestCommand(), $context);

                $query = updateQuery(
                    'sagas_store',
                    ['expiration_date' => \date('c', \strtotime('-1 hour'))]
                )
                    ->where(equalsCriteria("id", $id->id))
                    ->compile();

                yield $this->adapter->execute($query->sql(), $query->params());

                yield $this->sagaProvider->save($saga, $context);
                yield $this->sagaProvider->obtain($id, $context);
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function obtain(): void
    {
        Loop::run(
            function (): \Generator
            {
                $this->containerBuilder->get(Router::class);

                $context = new Context();
                $id      = TestSagaId::new(TestSaga::class);

                /** @var Saga $saga */
                $saga = yield $this->sagaProvider->start($id, new TestCommand(), $context);

                yield $this->sagaProvider->save($saga, $context);

                /** @var Saga $loadedSaga */
                $loadedSaga = yield $this->sagaProvider->obtain($id, $context);

                static::assertSame($saga->id()->id, $loadedSaga->id()->id);
            }
        );
    }
}
