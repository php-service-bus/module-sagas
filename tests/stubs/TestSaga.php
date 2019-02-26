<?php

/**
 * Saga pattern implementation module
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module\Tests\stubs;

use ServiceBus\Sagas\Configuration\Annotations\SagaHeader;
use ServiceBus\Sagas\Configuration\Annotations\SagaEventListener;
use ServiceBus\Sagas\Saga;

/**
 * @SagaHeader(
 *     idClass="ServiceBus\Sagas\Module\Tests\stubs\TestSagaId",
 *     containingIdProperty="requestId",
 *     expireDateModifier="+1 year"
 * )
 */
final class TestSaga extends Saga
{
    /**
     * @inheritDoc
     */
    public function start(object $command): void
    {

    }

    /**
     * @return void
     *
     * @throws \ServiceBus\Sagas\Exceptions\ChangeSagaStateFailed
     */
    public function doSomething(): void
    {
        $this->fire(new EmptyCommand());
    }

    /**
     * @noinspection PhpUnusedPrivateMethodInspection
     *
     * @SagaEventListener()
     *
     * @param EmptyEvent $event
     *
     * @return void
     *
     * @throws \ServiceBus\Sagas\Exceptions\ChangeSagaStateFailed
     */
    private function onEmptyEvent(/** @noinspection PhpUnusedParameterInspection */
        EmptyEvent $event
    ): void
    {
        $this->makeFailed('test reason');
    }
}
