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

use function Amp\call;
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

/**
 *
 */
final class SagasProviderTest extends TestCase
{
    /**
     * @var ContainerBuilder
     */
    private $containerBuilder;

    /**
     * @var DatabaseAdapter
     */
    private $adapter;

    /**
     * @var SagasProvider
     */
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
     *
     * @return void
     */
    public function updateNonexistentSaga(): void
    {
        $this->expectException(CantSaveUnStartedSaga::class);

        $sagaProvider = $this->sagaProvider;

        wait(
            call(
                static function() use ($sagaProvider): \Generator
                {
                    $testSaga = new TestSaga(TestSagaId::new(TestSaga::class));

                    yield $sagaProvider->save($testSaga, new Context());
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function startWithoutMetadata(): void
    {
        $this->expectException(SagaMetaDataNotFound::class);

        $sagaProvider = $this->sagaProvider;

        wait(
            call(
                static function() use ($sagaProvider): \Generator
                {
                    $id = TestSagaId::new(TestSaga::class);

                    yield $sagaProvider->start($id, new TestCommand(), new Context());
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function start(): void
    {
        $this->containerBuilder->get(Router::class);

        $sagaProvider = $this->sagaProvider;

        wait(
            call(
                static function() use ($sagaProvider): \Generator
                {
                    $id = TestSagaId::new(TestSaga::class);

                    /** @var Saga $saga */
                    $saga = yield $sagaProvider->start($id, new TestCommand(), new Context());

                    static::assertNotNull($saga);
                    static::assertInstanceOf(TestSaga::class, $saga);
                    static::assertSame($id, $saga->id());
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function startDuplicate(): void
    {
        $this->expectException(DuplicateSaga::class);

        $this->containerBuilder->get(Router::class);

        $sagaProvider = $this->sagaProvider;

        wait(
            call(
                static function() use ($sagaProvider): \Generator
                {
                    $id = TestSagaId::new(TestSaga::class);

                    yield $sagaProvider->start($id, new TestCommand(), new Context());
                    yield $sagaProvider->start($id, new TestCommand(), new Context());
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function startWithoutSchema(): void
    {
        $this->expectException(SagasStoreInteractionFailed::class);

        $sagaProvider     = $this->sagaProvider;
        $databaseAdapter  = $this->adapter;
        $containerBuilder = $this->containerBuilder;

        wait(
            call(
                static function() use ($sagaProvider, $databaseAdapter, $containerBuilder): \Generator
                {
                    yield $databaseAdapter->execute('DROP TABLE sagas_store');

                    $containerBuilder->get(Router::class);

                    yield $sagaProvider->start(TestSagaId::new(TestSaga::class), new TestCommand(), new Context());
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function obtainWithoutSchema(): void
    {
        $this->expectException(SagasStoreInteractionFailed::class);

        $this->containerBuilder->get(Router::class);

        $sagaProvider    = $this->sagaProvider;
        $databaseAdapter = $this->adapter;

        wait(
            call(
                static function() use ($sagaProvider, $databaseAdapter): \Generator
                {
                    yield $databaseAdapter->execute('DROP TABLE sagas_store');
                    yield $sagaProvider->obtain(TestSagaId::new(TestSaga::class), new Context());
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function saveWithoutSchema(): void
    {
        $this->expectException(SagasStoreInteractionFailed::class);

        $sagaProvider    = $this->sagaProvider;
        $databaseAdapter = $this->adapter;

        wait(
            call(
                static function() use ($sagaProvider, $databaseAdapter): \Generator
                {
                    yield $databaseAdapter->execute('DROP TABLE sagas_store');

                    $testSaga = new TestSaga(TestSagaId::new(TestSaga::class));

                    yield $sagaProvider->save($testSaga, new Context());
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function obtainNonexistentSaga(): void
    {
        $sagaProvider = $this->sagaProvider;

        wait(
            call(
                static function() use ($sagaProvider): \Generator
                {
                    static::assertNull(
                        yield $sagaProvider->obtain(TestSagaId::new(TestSaga::class), new Context())
                    );
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function obtainExpiredSaga(): void
    {
        $this->expectException(LoadedExpiredSaga::class);

        $this->containerBuilder->get(Router::class);

        $sagaProvider = $this->sagaProvider;

        wait(
            call(
                static function() use ($sagaProvider): \Generator
                {
                    $context = new Context();
                    $id      = TestSagaId::new(TestSaga::class);

                    /** @var Saga $saga */
                    $saga = yield $sagaProvider->start($id, new TestCommand(), $context);

                    writeReflectionPropertyValue($saga, 'expireDate', new \DateTimeImmutable('-1 hours'));

                    yield $sagaProvider->save($saga, $context);
                    yield $sagaProvider->obtain($id, $context);
                }
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     */
    public function obtain(): void
    {
        $this->containerBuilder->get(Router::class);

        $sagaProvider = $this->sagaProvider;

        wait(
            call(
                static function() use ($sagaProvider): \Generator
                {
                    $context = new Context();
                    $id      = TestSagaId::new(TestSaga::class);

                    /** @var Saga $saga */
                    $saga = yield $sagaProvider->start($id, new TestCommand(), $context);

                    yield $sagaProvider->save($saga, $context);

                    /** @var Saga $loadedSaga */
                    $loadedSaga = yield $sagaProvider->obtain($id, $context);

                    static::assertSame($saga->id()->id, $loadedSaga->id()->id);
                }
            )
        );
    }
}
