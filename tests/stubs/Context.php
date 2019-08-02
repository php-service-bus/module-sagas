<?php

/**
 * Saga pattern implementation module.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module\Tests\stubs;

use function ServiceBus\Common\uuid;
use Amp\Promise;
use Amp\Success;
use Psr\Log\LogLevel;
use ServiceBus\Common\Context\ServiceBusContext;
use ServiceBus\Common\Endpoint\DeliveryOptions;

/**
 *
 */
final class Context implements ServiceBusContext
{
    /**
     * {@inheritdoc}
     */
    public function isValid(): bool
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function violations(): array
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function delivery(object $message, ?DeliveryOptions $deliveryOptions = null): Promise
    {
        return new Success();
    }

    /**
     * {@inheritdoc}
     */
    public function headers(): array
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function logContextMessage(string $logMessage, array $extra = [], string $level = LogLevel::INFO): void
    {
    }

    /**
     * {@inheritdoc}
     */
    public function logContextThrowable(\Throwable $throwable, array $extra = [], string $level = LogLevel::ERROR): void
    {
    }

    /**
     * {@inheritdoc}
     */
    public function return(int $secondsDelay = 3): Promise
    {
        return new Success();
    }

    /**
     * {@inheritdoc}
     */
    public function operationId(): string
    {
        return uuid();
    }

    /**
     * {@inheritdoc}
     */
    public function traceId(): string
    {
        return uuid();
    }
}
