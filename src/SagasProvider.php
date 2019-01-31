<?php

/**
 * Sagas support module
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module;

use function Amp\call;
use Amp\Promise;
use ServiceBus\Common\Context\ServiceBusContext;
use function ServiceBus\Common\datetimeInstantiator;
use function ServiceBus\Common\invokeReflectionMethod;
use ServiceBus\Common\Messages\Command;
use function ServiceBus\Common\readReflectionPropertyValue;
use ServiceBus\Sagas\Configuration\SagaMetadata;
use ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga;
use ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound;
use ServiceBus\Sagas\Saga;
use ServiceBus\Sagas\SagaId;
use ServiceBus\Sagas\Store\Exceptions\LoadedExpiredSaga;
use ServiceBus\Sagas\Store\SagasStore;

/**
 * Sagas provider
 */
final class SagasProvider
{
    /**
     * Sagas store
     *
     * @var SagasStore
     */
    private $sagaStore;

    /**
     * Sagas meta data
     *
     * @var array<string, \ServiceBus\Sagas\Configuration\SagaMetadata>
     */
    private $sagaMetaDataCollection = [];

    /**
     * @param SagasStore $sagaStore
     */
    public function __construct(SagasStore $sagaStore)
    {
        $this->sagaStore = $sagaStore;
    }

    /**
     * Start a new saga
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param SagaId            $id
     * @param Command           $command
     * @param ServiceBusContext $context
     *
     * @return Promise<\ServiceBus\Sagas\Saga>
     *
     * @throws \ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound
     * @throws \ServiceBus\Sagas\Store\Exceptions\DuplicateSaga The specified saga has already been added
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     */
    public function start(SagaId $id, Command $command, ServiceBusContext $context): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(SagaId $id, Command $command, ServiceBusContext $context): \Generator
            {
                /** @var class-string<\ServiceBus\Sagas\Saga> $sagaClass */
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
            $id, $command, $context
        );
    }

    /**
     * Load saga
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param SagaId            $id
     * @param ServiceBusContext $context
     *
     * @return Promise<\ServiceBus\Sagas\Saga|null>
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while deserializing saga
     * @throws \ServiceBus\Sagas\Store\Exceptions\LoadedExpiredSaga Expired saga loaded
     */
    public function obtain(SagaId $id, ServiceBusContext $context): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(SagaId $id, ServiceBusContext $context): \Generator
            {
                /** @var \DateTimeImmutable $currentDatetime */
                $currentDatetime = datetimeInstantiator('NOW');

                /** @var Saga|null $saga */
                $saga = yield $this->sagaStore->obtain($id);

                if(null === $saga)
                {
                    return null;
                }

                /** Non-expired saga */
                if($saga->expireDate() > $currentDatetime)
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
            $id, $context
        );
    }

    /**
     * Save\update a saga
     *
     * @noinspection PhpDocRedundantThrowsInspection
     *
     * @param Saga              $saga
     * @param ServiceBusContext $context
     *
     * @return Promise  It doesn't return any result
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga Attempt to save un-started saga
     */
    public function save(Saga $saga, ServiceBusContext $context): Promise
    {
        /** @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args) */
        return call(
            function(Saga $saga, ServiceBusContext $context): \Generator
            {
                /** @var Saga|null $existsSaga */
                $existsSaga = yield $this->sagaStore->obtain($saga->id());

                if(null !== $existsSaga)
                {
                    yield from $this->doStore($saga, $context, false);

                    unset($existsSaga);

                    return;
                }

                throw new CantSaveUnStartedSaga($saga);
            },
            $saga, $context
        );
    }

    /**
     * Close expired saga
     *
     * @noinspection PhpDocMissingThrowsInspection
     *
     * @param Saga              $saga
     * @param ServiceBusContext $context
     *
     * @return \Generator It does not return any result
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     * @throws \ServiceBus\Sagas\Module\Exceptions\CantSaveUnStartedSaga Attempt to save un-started saga
     */
    private function doCloseExpired(Saga $saga, ServiceBusContext $context): \Generator
    {
        /**
         * @noinspection PhpUnhandledExceptionInspection
         * @var \ServiceBus\Sagas\SagaStatus $currentStatus
         */
        $currentStatus = readReflectionPropertyValue($saga, 'status');

        if(true === $currentStatus->inProgress())
        {
            /** @noinspection PhpUnhandledExceptionInspection */
            invokeReflectionMethod($saga, 'makeExpired');

            yield $this->save($saga, $context);
        }
    }

    /**
     * Execute add/update saga entry
     *
     * @noinspection PhpDocMissingThrowsInspection
     *
     * @param Saga              $saga
     * @param ServiceBusContext $context
     * @param bool              $isNew
     *
     * @return \Generator
     *
     * @throws \ServiceBus\Sagas\Store\Exceptions\DuplicateSaga The specified saga has already been added
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagasStoreInteractionFailed Database interaction error
     * @throws \ServiceBus\Sagas\Store\Exceptions\SagaSerializationError Error while serializing saga
     */
    private function doStore(Saga $saga, ServiceBusContext $context, bool $isNew): \Generator
    {
        /**
         * @noinspection PhpUnhandledExceptionInspection
         * @var array<int, \ServiceBus\Common\Messages\Command> $commands
         */
        $commands = invokeReflectionMethod($saga, 'firedCommands');

        /**
         * @noinspection PhpUnhandledExceptionInspection
         * @var array<int, \ServiceBus\Common\Messages\Event> $events
         */
        $events = invokeReflectionMethod($saga, 'raisedEvents');

        /** @var \Generator $generator */
        $generator = true === $isNew
            ? $this->sagaStore->save($saga)
            : $this->sagaStore->update($saga);

        yield $generator;

        /** @var array<mixed, \ServiceBus\Common\Messages\Message> $messages */
        $messages = \array_merge($commands, $events);

        $promises = [];

        /** @var \ServiceBus\Common\Messages\Message $message */
        foreach($messages as $message)
        {
            $promises[] = $context->delivery($message);
        }

        yield $promises;
    }

    /**
     * Receive saga meta data information
     *
     * @param string $sagaClass
     *
     * @return SagaMetadata
     *
     * @throws \ServiceBus\Sagas\Module\Exceptions\SagaMetaDataNotFound
     */
    private function extractSagaMetaData(string $sagaClass): SagaMetadata
    {
        if(true === isset($this->sagaMetaDataCollection[$sagaClass]))
        {
            return $this->sagaMetaDataCollection[$sagaClass];
        }

        throw new SagaMetaDataNotFound($sagaClass);
    }

    /**
     * Add meta data for specified saga
     * Called from the infrastructure layer using Reflection API
     *
     * @noinspection PhpUnusedPrivateMethodInspection
     *
     * @see          SagaMessagesRouterConfigurator::registerRoutes
     *
     * @param string       $sagaClass
     * @param SagaMetadata $metadata
     *
     * @return void
     */
    private function appendMetaData(string $sagaClass, SagaMetadata $metadata): void
    {
        $this->sagaMetaDataCollection[$sagaClass] = $metadata;
    }
}
