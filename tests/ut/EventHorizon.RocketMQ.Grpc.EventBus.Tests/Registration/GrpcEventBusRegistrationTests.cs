namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Registration;

public sealed class GrpcEventBusRegistrationTests
{
    [Fact]
    public void AddGrpcEventBus_PublisherOnlyAddsProducerAndDefaultEventBusWithoutAConsumer()
    {
        var services = new ServiceCollection();

        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(configureProducer: static _ => { });

        var eventBus = Assert.Single(services, static descriptor => descriptor.ServiceType == typeof(IEventBus));

        Assert.False(eventBus.IsKeyedService);
        Assert.DoesNotContain(services, static descriptor => descriptor.ServiceType == typeof(IGrpcPushConsumer));
    }

    [Fact]
    public void AddGrpcEventBus_ConsumerOnlyAddsPushConsumerAfterTheFirstHandlerWithoutAnEventBus()
    {
        var services = new ServiceCollection();
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer");

        Assert.DoesNotContain(services, static descriptor => descriptor.ServiceType == typeof(IEventBus));
        Assert.DoesNotContain(services, static descriptor => descriptor.ServiceType == typeof(IGrpcPushConsumer));

        eventBusBuilder.AddHandler<OrderCreatedHandler>();
        eventBusBuilder.AddHandler<OrderPlacedHandler>();

        Assert.DoesNotContain(services, static descriptor => descriptor.ServiceType == typeof(IEventBus));
        Assert.Single(services, static descriptor => descriptor.ServiceType == typeof(IGrpcPushConsumer));
        Assert.Contains(services, static descriptor => descriptor.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task AddGrpcEventBus_NamedPublishersAndConsumersRemainKeyedByTheirOwnRegistrationNames()
    {
        var services = new ServiceCollection();
        var orders = services
            .AddRocketMQGrpc("orders", ConfigureClient)
            .AddGrpcEventBus(
                configureConsumer: options => options.GroupName = "orders-consumer",
                configureProducer: static _ => { });
        var billing = services
            .AddRocketMQGrpc("billing", ConfigureClient)
            .AddGrpcEventBus(
                configureConsumer: options => options.GroupName = "billing-consumer",
                configureProducer: static _ => { });

        orders.AddHandler<OrderCreatedHandler>();
        billing.AddHandler<BillingCapturedHandler>();

        var eventBusDescriptors = services
            .Where(static descriptor => descriptor.ServiceType == typeof(IEventBus))
            .ToArray();

        Assert.Equal(2, eventBusDescriptors.Length);
        Assert.All(eventBusDescriptors, static descriptor => Assert.True(descriptor.IsKeyedService));
        Assert.Equal(
            ["billing", "orders"],
            eventBusDescriptors.Select(static descriptor => (string)descriptor.ServiceKey!).OrderBy(static key => key, StringComparer.Ordinal));
        Assert.Equal(2, services.Count(static descriptor => descriptor.ServiceType == typeof(IGrpcPushConsumer)));

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

        Assert.NotNull(provider.GetRequiredKeyedService<IEventBus>("orders"));
        Assert.NotNull(provider.GetRequiredKeyedService<IEventBus>("billing"));
        Assert.Null(provider.GetService<IEventBus>());
    }

    [Fact]
    public void AddGrpcEventBus_RejectsADuplicateDefaultIdentityThroughCoreRegistration()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services
                .AddRocketMQGrpc(ConfigureClient)
                .AddGrpcEventBus());

        Assert.Contains("<default>", exception.Message, StringComparison.Ordinal);
    }

    private static void ConfigureClient(GrpcClientOptions options) => options.Endpoint = "http://127.0.0.1:8081";
}
