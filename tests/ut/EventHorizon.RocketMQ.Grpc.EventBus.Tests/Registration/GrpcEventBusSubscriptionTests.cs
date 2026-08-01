namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Registration;

public sealed class GrpcEventBusSubscriptionTests
{
    [Fact]
    public async Task AddGrpcEventBus_MaterializesTheFinalDeterministicSubscriptionsAndWritesOneSummary()
    {
        var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddProvider(logs);
        });
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer");

        eventBusBuilder.AddHandler<OrderPlacedHandler>();
        eventBusBuilder.AddHandler<OrderCreatedHandler>();

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true,
        });
        var summary = Assert.Single(provider.GetServices<IHostedService>().OfType<GrpcEventBusSubscriptionSummaryHostedService>());

        await summary.StartAsync(TestContext.Current.CancellationToken);
        await summary.StartAsync(TestContext.Current.CancellationToken);

        var summaryLog = Assert.Single(logs.Entries, static entry =>
            entry.Category.StartsWith("EventHorizon.RocketMQ.Grpc.EventBus", StringComparison.Ordinal) &&
            entry.Message.Contains("EventBus subscriptions materialized", StringComparison.Ordinal));
        Assert.Equal(LogLevel.Information, summaryLog.LogLevel);
        Assert.Contains("orders-consumer", summaryLog.Message, StringComparison.Ordinal);
        Assert.Contains("orders: created || placed", summaryLog.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddGrpcEventBus_UsesThePostConfiguredConsumerGroupInTheSummary()
    {
        using var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddProvider(logs));
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "initial-consumer")
            .AddHandler<OrderCreatedHandler>();
        services.PostConfigureAll<GrpcPushConsumerOptions>(options => options.GroupName = "final-consumer");

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IGrpcPushConsumer>();
        var summary = Assert.Single(
            provider.GetServices<IHostedService>().OfType<GrpcEventBusSubscriptionSummaryHostedService>());

        await summary.StartAsync(TestContext.Current.CancellationToken);

        var summaryLog = Assert.Single(logs.Entries, static entry =>
            entry.Category.StartsWith("EventHorizon.RocketMQ.Grpc.EventBus", StringComparison.Ordinal) &&
            entry.Message.Contains("EventBus subscriptions materialized", StringComparison.Ordinal));
        Assert.Contains("final-consumer", summaryLog.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("initial-consumer", summaryLog.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddGrpcEventBus_RejectsApplicationManagedPushSubscriptions()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("manual");
            })
            .AddHandler<OrderCreatedHandler>();

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IGrpcPushConsumer>());

        Assert.Contains("EventBus owns all Push consumer subscriptions", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddGrpcEventBus_RejectsSubscriptionsAddedByPostConfigure()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<OrderCreatedHandler>();
        services.PostConfigureAll<GrpcPushConsumerOptions>(options => options.Subscribe("manual"));

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IGrpcPushConsumer>());

        Assert.Contains("owns all Push consumer subscriptions", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddGrpcEventBus_RejectsANullConsumerGroupConfiguredDirectly()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = null!)
            .AddHandler<OrderCreatedHandler>();

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IGrpcPushConsumer>());

        Assert.Contains("Consumer group name is required", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddGrpcEventBus_RejectsANullConsumerGroupAddedByPostConfigure()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<OrderCreatedHandler>();
        services.PostConfigureAll<GrpcPushConsumerOptions>(options => options.GroupName = null!);

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IGrpcPushConsumer>());

        Assert.Contains("Consumer group name is required", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddGrpcEventBus_UsesTheHandlerSnapshotOfEachServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer");
        eventBusBuilder.AddHandler<OrderCreatedHandler>();

        await using var firstProvider = services.BuildServiceProvider();
        eventBusBuilder.AddHandler<OrderPlacedHandler>();

        _ = firstProvider.GetRequiredService<IGrpcPushConsumer>();
        var firstOptions = firstProvider.GetRequiredService<IOptionsMonitor<GrpcPushConsumerOptions>>().CurrentValue;
        var firstSubscription = Assert.Single(firstOptions.Subscriptions);
        Assert.Equal("created", firstSubscription.Value.Expression);

        await using var secondProvider = services.BuildServiceProvider();
        _ = secondProvider.GetRequiredService<IGrpcPushConsumer>();
        var secondOptions = secondProvider.GetRequiredService<IOptionsMonitor<GrpcPushConsumerOptions>>().CurrentValue;
        var secondSubscription = Assert.Single(secondOptions.Subscriptions);
        Assert.Equal("created || placed", secondSubscription.Value.Expression);
    }

    [Fact]
    public async Task AddGrpcEventBus_OmitsTheSubscriptionSummaryWhenLoggingIsDisabled()
    {
        using var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddProvider(logs));
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer");
        eventBusBuilder.ConfigureLogging(options => options.Enabled = false);
        eventBusBuilder.AddHandler<OrderCreatedHandler>();

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IGrpcPushConsumer>();
        var summary = Assert.Single(
            provider.GetServices<IHostedService>().OfType<GrpcEventBusSubscriptionSummaryHostedService>());

        await summary.StartAsync(TestContext.Current.CancellationToken);

        Assert.DoesNotContain(logs.Entries, static entry =>
            entry.Category.StartsWith("EventHorizon.RocketMQ.Grpc.EventBus", StringComparison.Ordinal) &&
            entry.Message.Contains("subscriptions materialized", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AddGrpcEventBus_UsesAnAllTagSubscriptionWhenATopicContainsAnUntaggedRoute()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<GrpcDispatchRecorder>();
        GrpcPushConsumerOptions? capturedOptions = null;
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<OrderCreatedHandler>()
            .AddHandler<GrpcUntaggedHandler>();
        services.PostConfigureAll<GrpcPushConsumerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IGrpcPushConsumer>();

        var subscription = Assert.Single(Assert.IsType<GrpcPushConsumerOptions>(capturedOptions).Subscriptions);
        Assert.Equal("orders", subscription.Key);
        Assert.Equal("*", subscription.Value.Expression);
        Assert.Equal(FilterExpressionType.Tag, subscription.Value.Type);
    }

    private static void ConfigureClient(GrpcClientOptions options) => options.Endpoint = "http://127.0.0.1:8081";
}
