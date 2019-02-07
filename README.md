[![Build Status](https://travis-ci.org/php-service-bus/module-sagas.svg?branch=v3.0)](https://travis-ci.org/php-service-bus/module-sagas)
[![Code Coverage](https://scrutinizer-ci.com/g/php-service-bus/module-sagas/badges/coverage.png?b=v3.0)](https://scrutinizer-ci.com/g/php-service-bus/module-sagas/?branch=v3.0)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/php-service-bus/module-sagas/badges/quality-score.png?b=v3.0)](https://scrutinizer-ci.com/g/php-service-bus/module-sagas/?branch=v3.0)

## Table of contents
* [What is Saga?](https://github.com/php-service-bus/module-sagas#what-is-saga)
* [Field of use](https://github.com/php-service-bus/module-sagas#field-of-use)
* [Implementation features](https://github.com/php-service-bus/module-sagas#implementation-features)
* [Caveats](https://github.com/php-service-bus/module-sagas#caveats)
* [Installation](https://github.com/php-service-bus/module-sagas#installation)
* [Configuration](https://github.com/php-service-bus/module-sagas#saga-configuration)
* [Lifecycle](https://github.com/php-service-bus/module-sagas#lifecycle)
* [Creation](https://github.com/php-service-bus/module-sagas#creation)
* [Example](https://github.com/php-service-bus/module-sagas#example)

#### What is Saga?
Saga may be interpreted as any documented business process which consists of steps. Speaking technically, Saga is an Event Listener which listens to some [event](https://github.com/php-service-bus/common/blob/v3.0/src/Messages/Event.php) and performs an action based on that event. A good example is a flowchart with a decision symbol.

There are synchronous, asynchronous and mixed sagas (where some steps may be performed synchronously and some asynchronously). From personal experience only asynchronous sagas are worth implementing.

A little bit more on [Saga](https://microservices.io/patterns/data/saga.html).

#### Field of use
* Description of complicated business processes. A good example is electronic document management. Transfer of a document may take some time - up to weeks depending on many factors. The process itself consists of a dozen of steps including electronic signatures of 3 parties (3 party is the operator)
* Distributed transactions (emulated Atomicity): either each step would be finished successfully or a step-specific compensating action will be performed.

#### Implementation features
All the sagas in the framework are asynchronous and have their own state which is serialized and stored in database. Any variables expect closures may be used as saga state.
Any non-closed saga may be triggered starting from any of saga's steps if a corresponding event is received.

#### Caveats
The more sagas (and steps within them) you have the more documentation you need for them. Single saga may trigger other sagas heavily increasing complexity of business processes.
[Message based architecture](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Messaging.html) is very difficult to understand at the beginning (especially after "common" PHP programming) and requires a lot of responsibility.

#### Installation
```bash
composer req php-service-bus/module-sagas
composer req php-service-bus/storage-sql
```

```php
$module = SagaModule::withSqlStorage(DatabaseAdapter::class)
    ->enableAutoImportSagas([__DIR__ . '/src']);
```
Enable module:
```php
$bootstrap->applyModules($module);
```

#### Saga configuration
Sagas are configures through annotations [@SagaHeader](https://github.com/php-service-bus/sagas/blob/v3.0/src/Configuration/Annotations/SagaHeader.php) and [@SagaEventListener](https://github.com/php-service-bus/sagas/blob/v3.0/src/Configuration/Annotations/SagaEventListener.php).
Parameters:
 - ```idClass```: Saga identifier class namespace;
 - ```expireDateModifier```: Saga expiry interval;
 - ```containingIdProperty```: Field (in the event) where saga identifier should be found.
 Each event should be bound to specific saga instance (since one message may be received by different sagas) - that's why there should be an identifier in each of saga events. This parameter may be overridden per event listener class.

 Each event listener should be named like ```on{EventName}```, where *on* is a generic prefix and *{EventName}* is short class name.

 #### Lifecycle
 Saga execution starts on call of method [start()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Saga.php#L137), which will be called automatically (see example below). There are following methods (protected) available inside Saga instance:
- [fire()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Saga.php#L205): Dispatches a command;
- [raise()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Saga.php#L188): Dispatches an event;
- [makeCompleted()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Saga.php#L223): Closes saga marking it as successfully finished.
- [makeFailed()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Saga.php#L242): Closes saga marking it as failed.

On saga status change next events will be raised:
- [SagaCreated()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Contract/SagaCreated.php): Saga was created (started);
- [SagaStatusChanged()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Contract/SagaStatusChanged.php): Saga status was changed;
- [SagaClosed()](https://github.com/php-service-bus/sagas/blob/v3.0/src/Contract/SagaClosed.php): Saga was closed;

#### Creation
There is a specific provider created for more convenient operations with sagas - [SagaProvider](https://github.com/php-service-bus/module-sagas/blob/v3.0/src/SagasProvider.php). Each of its methods returns [Promise](https://github.com/amphp/amp/blob/v3.0/lib/Promise.php) object.
- [start()](https://github.com/php-service-bus/module-sagas/blob/v3.0/src/SagasProvider.php#L73): Creates and starts new saga firing a command;
- [obtain()](https://github.com/php-service-bus/module-sagas/blob/v3.0/src/SagasProvider.php#L115): Retrieves a saga instance from database;
- [save()](https://github.com/php-service-bus/module-sagas/blob/v3.0/src/SagasProvider.php#L166): Saves all changes in saga state and sends all saga events to transport.

#### Example

```php
final class ExampleSagaId extends SagaId
{

}

/**
 * @SagaHeader(
 *     idClass="\ExampleSagaId",
 *     expireDateModifier="+1 year",
 *     containingIdProperty="operationId"
 * )
 */
final class ExampleSaga extends Saga
{
    /**
     * @inheritDoc
     */
    public function start(Command $command): void
    {
        $this->fire(
            new SomeCommand(/** params */)
        );
    }

    /**
     * @SagaEventListener()
     *
     * @param SomeEvent $event
     *
     * @return void
     */
    private function onSomeEvent(SomeEvent $event): void
    {
        $this->fire(
            new NextCommand(/**  */)
        );
    }
}
```

```
/** @var \ServiceBus\Sagas\Saga **/
$saga = yield $sagaProvider->start(
    ExampleSagaId::new(ExampleSaga::class),
    new SomeStartCommand(),
    new KernelContext()
);
```

On saga creation each saga will be started and saved. It is not necessary to save a saga if there were no changes to its state after start. Saga should be saved if it was obtained from database.

*Saga uniqueness is ensured by a composite key which consists of an UUID and identifier class namespace. Identifier class namespace should be used here to allow launching different sagas with the same identifier (usually all of these are parts of a single business process).*
