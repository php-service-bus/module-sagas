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
use function ServiceBus\Common\datetimeInstantiator;
use function ServiceBus\Common\invokeReflectionMethod;
use function ServiceBus\Common\readReflectionPropertyValue;
use Amp\Promise;
use ServiceBus\Common\Context\ServiceBusContext;
use ServiceBus\Mutex\InMemoryMutexFactory;
use ServiceBus\Mutex\Lock;
use ServiceBus\Mutex\MutexFactory;
use ServiceBus\Sagas\Configuration\SagaMetadata;
use ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga;
use ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound;
use ServiceBus\Sagas\Saga;
use ServiceBus\Sagas\SagaId;
use ServiceBus\Sagas\Store\Exceptions\LoadedExpiredSaga;
use ServiceBus\Sagas\Store\SagasStore;

/**
 * Sagas provider.
 */
final class SagasProvider
{
    /**
     * Sagas store.
     *
     * @var SagasStore
     */
    private $sagaStore;

    /**
     * @var MutexFactory
     */
    private $mutexFactory;

    /**
     * Sagas meta data.
     *
     * @psalm-var array<string, \ServiceBus\Sagas\Configuration\SagaMetadata>
     *
     * @var \ServiceBus\Sagas\Configuration\SagaMetadata[]
     */
    private $sagaMetaDataCollection = [];

    /**
     * @psalm-var array<string, \ServiceBus\Mutex\Lock>
     *
     * @var Lock[]
     */
    private $lockCollection = [];

    /**
     * SagasProvider constructor.
     *
     * @param SagasStore        $sagaStore
     * @param MutexFactory|null $mutexFactory
     */
    public function __construct(SagasStore $sagaStore, ?MutexFactory $mutexFactory = null)
    {
        $this->sagaStore    = $sagaStore;
        $this->mutexFactory = $mutexFactory ?? new InMemoryMutexFactory();
    }

    public function __destruct()
    {
        /** @var \ServiceBus\Mutex\Lock $lock */
        foreach ($this->lockCollection as $lock)
        {
            $lock->release();
        }
    }

    /**
     * Start a new saga.
     *
     * @noinspection   PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion
     *
     * @param SagaId            $id
     * @param object            $command
     * @param ServiceBusContext $context
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\DuplicateSaga The specified saga has already been added
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound
     *
     * @return Promise<\ServiceBus\Sagas\Saga>
     */
    public function start(SagaId $id, object $command, ServiceBusContext $context): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(SagaId $id, object $command, ServiceBusContext $context): \Generator
            {
                yield from $this->setupMutex($id);

                /** @psalm-var class-string<\ServiceBus\Sagas\Saga> $sagaClass */
                $sagaClass = $id->sagaClass;

                $sagaMetaData = $this->extractSagaMetaData($sagaClass);

                /** @var \DateTimeImmutable $expireDate */
                $expireDate = datetimeInstantiator($sagaMetaData->expireDateModifier);

                /** @var Saga $saga */
                $saga = new $sagaClass($id, $expireDate);
                $saga->start($command);

                yield from $this->doStore($saga, $context, true);

                unset($sagaClass, $sagaMetaData, $expireDate);

                return $saga;
            },
            $id,
            $command,
            $context
        );
    }

    /**
     * Load saga.
     *
     * @noinspection   PhpDocRedundantThrowsInspection
     * @psalm-suppress MixedTypeCoercion
     *
     * @param SagaId            $id
     * @param ServiceBusContext $context
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while deserializing saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\LoadedExpiredSaga Expired saga loaded
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     *
     * @return Promise<\ServiceBus\Sagas\Saga|null>
     */
    public function obtain(SagaId $id, ServiceBusContext $context): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(SagaId $id, ServiceBusContext $context): \Generator
            {
                /** @var \DateTimeImmutable $currentDatetime */
                $currentDatetime = datetimeInstantiator('NOW');

                yield from $this->setupMutex($id);

                /**
                 * @psalm-suppress TooManyTemplateParams
                 *
                 * @var Saga|null $saga
                 */
                $saga = yield $this->sagaStore->obtain($id);

                if (null === $saga)
                {
                    return null;
                }

                /** Non-expired saga */
                if ($saga->expireDate() > $currentDatetime)
                {
                    unset($currentDatetime);

                    return $saga;
                }

                yield from $this->doCloseExpired($saga, $context);

                unset($saga);

                throw new LoadedExpiredSaga(
                    \sprintf('Unable to load the saga (ID: "%s") whose lifetime has expired', $id)
                );
            },
            $id,
            $context
        );
    }

    /**
     * Save\update a saga.
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param Saga              $saga
     * @param ServiceBusContext $context
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga Attempt to save un-started saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     *
     * @return Promise  It doesn't return any result
     */
    public function save(Saga $saga, ServiceBusContext $context): Promise
    {
        /** @psalm-suppress InvalidArgument */
        return call(
            function(Saga $saga, ServiceBusContext $context): \Generator
            {
                /**
                 * @psalm-suppress TooManyTemplateParams
                 *
                 * @var Saga|null $existsSaga
                 */
                $existsSaga = yield $this->sagaStore->obtain($saga->id());

                if (null !== $existsSaga)
                {
                    yield from $this->doStore($saga, $context, false);

                    unset($existsSaga);

                    return;
                }

                throw CantSaveUnStartedSaga::create($saga);
            },
            $saga,
            $context
        );
    }

    /**
     * Close expired saga.
     *
     * @noinspection PhpDocMissingThrowsInspection
     *
     * @param Saga              $saga
     * @param ServiceBusContext $context
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga Attempt to save un-started saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     *
     * @return \Generator It does not return any result
     */
    private function doCloseExpired(Saga $saga, ServiceBusContext $context): \Generator
    {
        /**
         * @noinspection PhpUnhandledExceptionInspection
         *
         * @var \ServiceBus\Sagas\SagaStatus $currentStatus
         */
        $currentStatus = readReflectionPropertyValue($saga, 'status');

        if (true === $currentStatus->inProgress())
        {
            /** @noinspection PhpUnhandledExceptionInspection */
            invokeReflectionMethod($saga, 'makeExpired');

            yield $this->save($saga, $context);
        }
    }

    /**
     * Execute add/update saga entry.
     *
     * @noinspection PhpDocMissingThrowsInspection
     *
     * @param Saga              $saga
     * @param ServiceBusContext $context
     * @param bool              $isNew
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\DuplicateSaga The specified saga has already been added
     *
     * @return \Generator
     */
    private function doStore(Saga $saga, ServiceBusContext $context, bool $isNew): \Generator
    {
        /**
         * @noinspection PhpUnhandledExceptionInspection
         * @psalm-var    array<int, object> $commands
         *
         * @var object[] $commands
         */
        $commands = invokeReflectionMethod($saga, 'firedCommands');

        /**
         * @noinspection PhpUnhandledExceptionInspection
         * @psalm-var    array<int, object> $events
         *
         * @var object[] $events
         */
        $events = invokeReflectionMethod($saga, 'raisedEvents');

        /** @var \Generator $generator */
        $generator = true === $isNew
            ? $this->sagaStore->save($saga)
            : $this->sagaStore->update($saga);

        yield $generator;

        /**
         * @var object[] $messages
         * @psalm-var array<array-key, object> $messages
         */
        $messages = \array_merge($commands, $events);

        $promises = [];

        /** @var object $message */
        foreach ($messages as $message)
        {
            $promises[] = $context->delivery($message);
        }

        yield $promises;

        /** remove mutex */
        $this->releaseMutex($saga->id());
    }

    /**
     * Receive saga meta data information.
     *
     * @param string $sagaClass
     *
     * @throws \ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound
     *
     * @return SagaMetadata
     */
    private function extractSagaMetaData(string $sagaClass): SagaMetadata
    {
        if (true === isset($this->sagaMetaDataCollection[$sagaClass]))
        {
            return $this->sagaMetaDataCollection[$sagaClass];
        }

        throw SagaMetaDataNotFound::create($sagaClass);
    }

    /**
     * Add meta data for specified saga
     * Called from the infrastructure layer using Reflection API.
     *
     * @noinspection PhpUnusedPrivateMethodInspection
     *
     * @param string       $sagaClass
     * @param SagaMetadata $metadata
     *
     * @return void
     *
     * @see          SagaMessagesRouterConfigurator::registerRoutes
     */
    private function appendMetaData(string $sagaClass, SagaMetadata $metadata): void
    {
        $this->sagaMetaDataCollection[$sagaClass] = $metadata;
    }

    /**
     * Setup mutes on saga.
     *
     * @param SagaId $id
     *
     * @return \Generator
     */
    private function setupMutex(SagaId $id): \Generator
    {
        $mutexKey = self::createMutexKey($id);

        $mutex = $this->mutexFactory->create($mutexKey);

        $this->lockCollection[$mutexKey] = yield $mutex->acquire();
    }

    /**
     * Remove lock from saga.
     *
     * @param SagaId $id
     *
     * @return void
     */
    private function releaseMutex(SagaId $id): void
    {
        $mutexKey = self::createMutexKey($id);

        /** @var Lock|null $lock */
        $lock = $this->lockCollection[$mutexKey] ?? null;

        if (null !== $lock)
        {
            unset($this->lockCollection[$mutexKey]);

            $lock->release();
        }
    }

    /**
     * @param SagaId $id
     *
     * @return string
     */
    private static function createMutexKey(SagaId $id): string
    {
        return \sha1(\sprintf('%s:%s', $id->id, $id->sagaClass));
    }
}
