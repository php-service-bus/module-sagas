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
use function ServiceBus\Sagas\createMutexKey;
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
    /** @var SagasStore */
    private $sagaStore;

    /** @var MutexFactory */
    private $mutexFactory;

    /**
     * Sagas meta data.
     *
     * @psalm-var array<string, \ServiceBus\Sagas\Configuration\SagaMetadata>
     *
     * @var SagaMetadata[]
     */
    private $sagaMetaDataCollection = [];

    /**
     * @psalm-var array<string, \ServiceBus\Mutex\Lock>
     *
     * @var Lock[]
     */
    private $lockCollection = [];

    public function __construct(SagasStore $sagaStore, ?MutexFactory $mutexFactory = null)
    {
        $this->sagaStore    = $sagaStore;
        $this->mutexFactory = $mutexFactory ?? new InMemoryMutexFactory();
    }

    public function __destruct()
    {
        foreach ($this->lockCollection as $lock)
        {
            yield $lock->release();
        }
    }

    /**
     * Start a new saga.
     *
     * Returns \ServiceBus\Sagas\Saga
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound
     * @throws \ServiceBus\Sagas\Store\Exceptions\DuplicateSaga The specified saga has already been added
     */
    public function start(SagaId $id, object $command, ServiceBusContext $context): Promise
    {
        return call(
            function () use ($id, $command, $context): \Generator
            {
                yield from $this->setupMutex($id);

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
            }
        );
    }

    /**
     * Load saga.
     *
     * Returns \ServiceBus\Sagas\Saga|null
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\LoadedExpiredSaga Expired saga loaded
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while deserializing saga
     */
    public function obtain(SagaId $id, ServiceBusContext $context): Promise
    {
        return call(
            function () use ($id, $context): \Generator
            {
                /** @var \DateTimeImmutable $currentDatetime */
                $currentDatetime = datetimeInstantiator('NOW');

                yield from $this->setupMutex($id);

                /** @var Saga|null $saga */
                $saga = yield $this->sagaStore->obtain($id);

                if ($saga === null)
                {
                    yield from $this->releaseMutex($id);

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
                    \sprintf('Unable to load the saga (ID: "%s") whose lifetime has expired', $id->toString())
                );
            }
        );
    }

    /**
     * Save\update a saga.
     *
     * @throws \ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga Attempt to save un-started saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     */
    public function save(Saga $saga, ServiceBusContext $context): Promise
    {
        return call(
            function () use ($saga, $context): \Generator
            {
                /** @var Saga|null $existsSaga */
                $existsSaga = yield $this->sagaStore->obtain($saga->id());

                if ($existsSaga !== null)
                {
                    yield from $this->doStore($saga, $context, false);

                    unset($existsSaga);

                    return;
                }

                throw CantSaveUnStartedSaga::create($saga);
            }
        );
    }

    /**
     * Close expired saga.
     *
     * @throws \ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga Attempt to save un-started saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     */
    private function doCloseExpired(Saga $saga, ServiceBusContext $context): \Generator
    {
        /** @var \ServiceBus\Sagas\SagaStatus $currentStatus */
        $currentStatus = readReflectionPropertyValue($saga, 'status');

        if ($currentStatus->inProgress() === true)
        {
            invokeReflectionMethod($saga, 'makeExpired');

            yield $this->save($saga, $context);
        }
    }

    /**
     * Execute add/update saga entry.
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\DuplicateSaga The specified saga has already been added
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     */
    private function doStore(Saga $saga, ServiceBusContext $context, bool $isNew): \Generator
    {
        /**
         * @psalm-var    array<int, object> $commands
         *
         * @var object[] $commands
         */
        $commands = invokeReflectionMethod($saga, 'firedCommands');

        /**
         * @psalm-var    array<int, object> $events
         *
         * @var object[] $events
         */
        $events = invokeReflectionMethod($saga, 'raisedEvents');

        $isNew === true
            ? yield $this->sagaStore->save($saga)
            : yield $this->sagaStore->update($saga);

        /**
         * @var object[] $messages
         * @psalm-var array<array-key, object> $messages
         */
        $messages = \array_merge($commands, $events);

        $promises = [];

        foreach ($messages as $message)
        {
            $promises[] = $context->delivery($message);
        }

        yield $promises;

        unset($promises, $commands, $events);

        /** remove mutex */
        yield from $this->releaseMutex($saga->id());
    }

    /**
     * Receive saga meta data information.
     *
     * @throws \ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound
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
     * @see          SagaMessagesRouterConfigurator::registerRoutes
     */
    private function appendMetaData(string $sagaClass, SagaMetadata $metadata): void
    {
        $this->sagaMetaDataCollection[$sagaClass] = $metadata;
    }

    /**
     * Setup mutes on saga.
     */
    private function setupMutex(SagaId $id): \Generator
    {
        $mutexKey = createMutexKey($id);

        if (\array_key_exists($mutexKey, $this->lockCollection) === false)
        {
            $mutex = $this->mutexFactory->create($mutexKey);

            /** @var \ServiceBus\Mutex\Lock $lock */
            $lock = yield $mutex->acquire();

            $this->lockCollection[$mutexKey] = $lock;
        }
    }

    /**
     * Remove lock from saga.
     */
    private function releaseMutex(SagaId $id): \Generator
    {
        $mutexKey = createMutexKey($id);

        $lock = $this->lockCollection[$mutexKey] ?? null;

        if ($lock !== null)
        {
            unset($this->lockCollection[$mutexKey]);

            yield $lock->release();
        }
    }
}
