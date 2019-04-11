<?php

/**
 * Saga pattern implementation module.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module\Exceptions;

use ServiceBus\Sagas\Saga;

/**
 *
 */
final class CantSaveUnStartedSaga extends \LogicException
{
    /**
     * @param Saga $saga
     *
     * @return self
     */
    public static function create(Saga $saga): self
    {
        return new self(
            \sprintf(
                'Saga with identifier "%s:%s" not exists. Please, use start() method for saga creation',
                $saga->id()->toString(),
                \get_class($saga->id())
            )
        );
    }
}
