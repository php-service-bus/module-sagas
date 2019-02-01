<?php

/**
 * Saga pattern implementation module
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Sagas\Module;

use ServiceBus\AnnotationsReader\AnnotationsReader;
use function ServiceBus\Common\canonicalizeFilesPath;
use function ServiceBus\Common\extractNamespaceFromFile;
use ServiceBus\Common\Module\ServiceBusModule;
use function ServiceBus\Common\searchFiles;
use ServiceBus\MessagesRouter\ChainRouterConfigurator;
use ServiceBus\MessagesRouter\Router;
use ServiceBus\Sagas\Configuration\Annotations\SagaAnnotationBasedConfigurationLoader;
use ServiceBus\Sagas\Configuration\DefaultEventListenerProcessorFactory;
use ServiceBus\Sagas\Configuration\EventListenerProcessorFactory;
use ServiceBus\Sagas\Configuration\SagaConfigurationLoader;
use ServiceBus\Sagas\Saga;
use ServiceBus\Sagas\Store\SagasStore;
use ServiceBus\Sagas\Store\Sql\SQLSagaStore;
use ServiceBus\Storage\Common\DatabaseAdapter;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

/**
 *
 */
final class SagaModule implements ServiceBusModule
{
    /**
     * @var string
     */
    private $sagaStoreServiceId;

    /**
     * @var string
     */
    private $databaseAdapterServiceId;

    /**
     * @var array<array-key, class-string<\ServicesBus\Sagas\Saga>>
     */
    private $sagasToRegister = [];

    /**
     * @var string|null
     */
    private $configurationLoaderServiceId;

    /**
     * @param string      $databaseAdapterServiceId
     * @param string|null $configurationLoaderServiceId If not specified, the default annotation-based configurator
     *                                                  will be used
     *
     * @return self
     *
     * @throws \LogicException The component "php-service-bus/storage-sql" was not installed
     * @throws \LogicException The component "php-service-bus/annotations-reader" was not installed
     */
    public static function withSqlStorage(
        string $databaseAdapterServiceId,
        ?string $configurationLoaderServiceId = null
    ): self
    {
        if(false === \interface_exists(DatabaseAdapter::class))
        {
            throw new \LogicException('The component "php-service-bus/storage-sql" was not installed');
        }

        if(null === $configurationLoaderServiceId && false === \interface_exists(AnnotationsReader::class))
        {
            throw new \LogicException('The component "php-service-bus/annotations-reader" was not installed');
        }

        return new self(
            SQLSagaStore::class,
            $databaseAdapterServiceId,
            $configurationLoaderServiceId
        );
    }

    /**
     * @param string      $storeImplementationServiceId
     * @param string      $databaseAdapterServiceId
     * @param string|null $configurationLoaderServiceId If not specified, the default annotation-based configurator
     *                                                  will be used
     *
     * @throws \LogicException The component "php-service-bus/annotations-reader" was not installed
     *
     * @return self
     */
    public static function withCustomStore(
        string $storeImplementationServiceId,
        string $databaseAdapterServiceId,
        ?string $configurationLoaderServiceId = null
    ): self
    {
        if(null === $configurationLoaderServiceId && false === \interface_exists(AnnotationsReader::class))
        {
            throw new \LogicException('The component "php-service-bus/annotations-reader" was not installed');
        }

        return new self(
            $storeImplementationServiceId,
            $databaseAdapterServiceId,
            $configurationLoaderServiceId
        );
    }

    /**
     * All sagas from the specified directories will be registered automatically
     *
     * @noinspection PhpDocMissingThrowsInspection
     *
     * Note: All files containing user-defined functions must be excluded
     * Note: Increases start time because of the need to scan files
     *
     * @param array<array-key, string> $directories
     * @param array<array-key, string> $excludedFiles
     *
     * @return $this
     */
    public function enableAutoImportSagas(array $directories, array $excludedFiles = []): self
    {
        $excludedFiles = canonicalizeFilesPath($excludedFiles);
        $files         = searchFiles($directories, '/\.php/i');

        /** @var \SplFileInfo $file */
        foreach($files as $file)
        {
            $filePath = $file->getRealPath();

            if(true === \in_array($filePath, $excludedFiles, true))
            {
                continue;
            }

            /** @noinspection PhpUnhandledExceptionInspection */
            $class = extractNamespaceFromFile($filePath);

            if(null !== $class && true === \is_a($class, Saga::class, true))
            {
                /** @var class-string<\ServiceBus\Sagas\Saga> $class */

                $this->configureSaga($class);
            }
        }

        return $this;
    }

    /**
     * Enable sagas
     *
     * @param array<array-key, class-string<\ServicesBus\Sagas\Saga>> $sagas
     *
     * @return $this
     */
    public function configureSagas(array $sagas): self
    {
        foreach($sagas as $saga)
        {
            $this->configureSaga($saga);
        }

        return $this;
    }

    /**
     * Enable specified saga
     *
     * @psalm-param class-string<\ServicesBus\Sagas\Saga> $sagaClass
     *
     * @param string $sagaClass
     *
     * @return $this
     */
    public function configureSaga(string $sagaClass): self
    {
        $this->sagasToRegister[\sha1($sagaClass)] = $sagaClass;

        return $this;
    }

    /**
     * @inheritDoc
     */
    public function boot(ContainerBuilder $containerBuilder): void
    {
        $containerBuilder->setParameter('service_bus.sagas.list', $this->sagasToRegister);

        $this->registerSagaStore($containerBuilder);
        $this->registerSagasProvider($containerBuilder);

        if(null === $this->configurationLoaderServiceId)
        {
            $this->registerDefaultConfigurationLoader($containerBuilder);

            $this->configurationLoaderServiceId = SagaConfigurationLoader::class;
        }

        $this->registerRoutesConfigurator($containerBuilder);
    }

    /**
     * @noinspection PhpDocMissingThrowsInspection
     *
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function registerRoutesConfigurator(ContainerBuilder $containerBuilder): void
    {
        if(false === $containerBuilder->hasDefinition(ChainRouterConfigurator::class))
        {
            $containerBuilder->addDefinitions([
                    ChainRouterConfigurator::class => new Definition(ChainRouterConfigurator::class)
                ]
            );
        }

        /** @noinspection PhpUnhandledExceptionInspection */
        $routerConfiguratorDefinition = $containerBuilder->getDefinition(ChainRouterConfigurator::class);

        if(false === $containerBuilder->hasDefinition(Router::class))
        {
            $containerBuilder->addDefinitions([Router::class => new Definition(Router::class)]);
        }

        /** @noinspection PhpUnhandledExceptionInspection */
        $routerDefinition = $containerBuilder->getDefinition(Router::class);
        $routerDefinition->setConfigurator(
            [new Reference(ChainRouterConfigurator::class), 'configure']
        );

        $sagaRoutingConfiguratorDefinition = (new Definition(SagaMessagesRouterConfigurator::class))
            ->setArguments([
                new Reference(SagasProvider::class),
                new Reference(SagaConfigurationLoader::class),
                '%service_bus.sagas.list%'
            ]);

        $containerBuilder->addDefinitions([SagaMessagesRouterConfigurator::class => $sagaRoutingConfiguratorDefinition]);

        /** @noinspection PhpUnhandledExceptionInspection */
        $routerConfiguratorDefinition->addMethodCall(
            'addConfigurator',
            [new Reference(SagaMessagesRouterConfigurator::class)]
        );
    }

    /**
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function registerSagasProvider(ContainerBuilder $containerBuilder): void
    {
        $sagasProviderDefinition = (new Definition(SagasProvider::class))
            ->setArguments([new Reference(SagasStore::class)]);

        $containerBuilder->addDefinitions([SagasProvider::class => $sagasProviderDefinition]);
    }

    /**
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function registerSagaStore(ContainerBuilder $containerBuilder): void
    {
        if(true === $containerBuilder->hasDefinition(SagasStore::class))
        {
            return;
        }

        $sagaStoreDefinition = (new Definition($this->sagaStoreServiceId))
            ->setArguments([new Reference($this->databaseAdapterServiceId)]);

        $containerBuilder->addDefinitions([SagasStore::class => $sagaStoreDefinition]);
    }

    /**
     * @param ContainerBuilder $containerBuilder
     *
     * @return void
     */
    private function registerDefaultConfigurationLoader(ContainerBuilder $containerBuilder): void
    {
        if(true === $containerBuilder->hasDefinition(SagaConfigurationLoader::class))
        {
            return;
        }

        /** Event listener factory */
        $listenerFactoryDefinition = (new Definition(DefaultEventListenerProcessorFactory::class))
            ->setArguments([new Reference(SagasStore::class)]);

        $containerBuilder->addDefinitions([EventListenerProcessorFactory::class => $listenerFactoryDefinition]);

        /** Configuration loader */
        $configurationLoaderDefinition = (new Definition(SagaAnnotationBasedConfigurationLoader::class))
            ->setArguments([new Reference(EventListenerProcessorFactory::class)]);

        $containerBuilder->addDefinitions([SagaConfigurationLoader::class => $configurationLoaderDefinition]);

        $this->configurationLoaderServiceId = SagaConfigurationLoader::class;
    }

    /**
     * @param string      $sagaStoreServiceId
     * @param string      $databaseAdapterServiceId
     * @param string|null $configurationLoaderServiceId
     */
    private function __construct(
        string $sagaStoreServiceId,
        string $databaseAdapterServiceId,
        ?string $configurationLoaderServiceId = null
    )
    {
        $this->sagaStoreServiceId           = $sagaStoreServiceId;
        $this->databaseAdapterServiceId     = $databaseAdapterServiceId;
        $this->configurationLoaderServiceId = $configurationLoaderServiceId;
    }
}
