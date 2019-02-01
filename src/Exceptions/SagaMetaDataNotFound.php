<?php

/**
 * Saga pattern implementation module
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module\Exceptions;

/**
 *
 */
final class SagaMetaDataNotFound extends \RuntimeException
{
    /**
     * @param string $sagaClass
     */
    public function __construct(string $sagaClass)
    {
        parent::__construct(
            \sprintf(
                'Meta data of the saga "%s" not found. The saga was not configured',
                $sagaClass
            )
        );
    }
}
