namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Consumer;

public sealed class RemotingEventBusConsumerConfigurationTests
{
    [Fact]
    public async Task FirstHandler_AddsOnePushConsumerWithDeterministicTagSubscriptions()
    {
        var services = new ServiceCollection();
        RemotingPushConsumerOptions? capturedOptions = null;
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>()
            .AddHandler<RemotingSecondTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();

        var options = Assert.IsType<RemotingPushConsumerOptions>(capturedOptions);
        Assert.Equal(ConsumerMode.Clustering, options.ConsumerMode);
        Assert.Equal(1, options.ConsumeMessageBatchSize);
        var subscription = Assert.Single(options.Subscriptions);
        Assert.Equal("orders", subscription.Key);
        Assert.Equal("cancelled || submitted", subscription.Value.Expression);
        Assert.Equal(FilterExpressionType.Tag, subscription.Value.Type);
    }

    [Fact]
    public async Task FirstHandler_ForcesOneMessageDispatchWithoutChangingReceivePrefetch()
    {
        var services = new ServiceCollection();
        RemotingPushConsumerOptions? capturedOptions = null;
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options =>
            {
                options.GroupName = "orders-consumer";
                options.BatchSize = 48;
                options.ConsumeMessageBatchSize = 12;
            })
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();

        var options = Assert.IsType<RemotingPushConsumerOptions>(capturedOptions);
        Assert.Equal(48, options.BatchSize);
        Assert.Equal(1, options.ConsumeMessageBatchSize);
    }

    [Fact]
    public async Task UntaggedRoute_UsesTheAllTagSubscriptionForItsTopic()
    {
        var services = new ServiceCollection();
        RemotingPushConsumerOptions? capturedOptions = null;
        services.AddSingleton<RemotingDispatchRecorder>();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>()
            .AddHandler<RemotingUntaggedTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();

        var subscription = Assert.Single(Assert.IsType<RemotingPushConsumerOptions>(capturedOptions).Subscriptions);
        Assert.Equal("orders", subscription.Key);
        Assert.Equal("*", subscription.Value.Expression);
        Assert.Equal(FilterExpressionType.Tag, subscription.Value.Type);
    }

    [Fact]
    public async Task FirstHandler_RejectsBroadcastingMode()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options =>
            {
                options.GroupName = "orders-consumer";
                options.ConsumerMode = ConsumerMode.Broadcasting;
            })
            .AddHandler<RemotingTestHandler>();

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("clustering", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task FirstHandler_RejectsManualSubscriptions()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("manual", new FilterExpression("manual"));
            })
            .AddHandler<RemotingTestHandler>();

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("owns Push consumer subscriptions", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FirstHandler_RejectsOrderlyConsumption()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options =>
            {
                options.GroupName = "orders-consumer";
                options.ConsumeOrderly = true;
            })
            .AddHandler<RemotingTestHandler>();

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("orderly", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FirstHandler_RejectsBatchSizeChangedByPostConfigure()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => options.ConsumeMessageBatchSize = 2);

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("exactly one message", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FirstHandler_RejectsOrderlyConsumptionEnabledByPostConfigure()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => options.ConsumeOrderly = true);

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("orderly", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FirstHandler_RejectsSubscriptionsAddedByPostConfigure()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => options.Subscribe("manual"));

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("owns Push consumer subscriptions", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FirstHandler_RejectsANullConsumerGroupConfiguredDirectly()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = null!)
            .AddHandler<RemotingTestHandler>();

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("Consumer group name is required", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FirstHandler_RejectsANullConsumerGroupAddedByPostConfigure()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => options.GroupName = null!);

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IRemotingPushConsumer>());

        Assert.Contains("Consumer group name is required", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FirstHandler_UsesTheHandlerSnapshotOfEachServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var eventBusBuilder = services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer");
        eventBusBuilder.AddHandler<RemotingTestHandler>();

        await using var firstProvider = services.BuildServiceProvider();
        eventBusBuilder.AddHandler<RemotingSecondTestHandler>();

        _ = firstProvider.GetRequiredService<IRemotingPushConsumer>();
        var firstOptions = firstProvider.GetRequiredService<IOptionsMonitor<RemotingPushConsumerOptions>>().CurrentValue;
        var firstSubscription = Assert.Single(firstOptions.Subscriptions);
        Assert.Equal("submitted", firstSubscription.Value.Expression);

        await using var secondProvider = services.BuildServiceProvider();
        _ = secondProvider.GetRequiredService<IRemotingPushConsumer>();
        var secondOptions = secondProvider.GetRequiredService<IOptionsMonitor<RemotingPushConsumerOptions>>().CurrentValue;
        var secondSubscription = Assert.Single(secondOptions.Subscriptions);
        Assert.Equal("cancelled || submitted", secondSubscription.Value.Expression);
    }

    [Fact]
    public async Task SubscriptionSummary_WritesOneInformationEntryWithoutMessagePayloads()
    {
        var services = new ServiceCollection();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();
        var summaryHostedService = provider.GetServices<IHostedService>()
            .OfType<RemotingEventBusSubscriptionSummaryHostedService>()
            .Single();

        await summaryHostedService.StartAsync(TestContext.Current.CancellationToken);
        await summaryHostedService.StartAsync(TestContext.Current.CancellationToken);

        var entries = loggerProvider.Entries
            .Where(static entry => entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal))
            .ToArray();
        var summary = Assert.Single(entries);
        Assert.Equal(LogLevel.Information, summary.Level);
        Assert.Contains("orders-consumer", summary.Message, StringComparison.Ordinal);
        Assert.Contains("orders: submitted", summary.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SubscriptionSummary_UsesThePostConfiguredConsumerGroup()
    {
        var services = new ServiceCollection();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "initial-consumer")
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => options.GroupName = "final-consumer");

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();
        var summary = Assert.Single(
            provider.GetServices<IHostedService>().OfType<RemotingEventBusSubscriptionSummaryHostedService>());

        await summary.StartAsync(TestContext.Current.CancellationToken);

        var summaryLog = Assert.Single(loggerProvider.Entries, static entry =>
            entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal) &&
            entry.Message.Contains("EventBus subscriptions materialized", StringComparison.Ordinal));
        Assert.Contains("final-consumer", summaryLog.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("initial-consumer", summaryLog.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SubscriptionSummary_IsolatedForEachNamedRegistration()
    {
        var services = new ServiceCollection();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services
            .AddRocketMQRemoting("orders", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();
        services
            .AddRocketMQRemoting("billing", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "billing-consumer")
            .AddHandler<RemotingSecondTestHandler>();

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredKeyedService<IRemotingPushConsumer>("orders");
        _ = provider.GetRequiredKeyedService<IRemotingPushConsumer>("billing");
        var summaryHostedServices = provider.GetServices<IHostedService>()
            .OfType<RemotingEventBusSubscriptionSummaryHostedService>()
            .ToArray();

        foreach (var summaryHostedService in summaryHostedServices)
        {
            await summaryHostedService.StartAsync(TestContext.Current.CancellationToken);
        }

        var entries = loggerProvider.Entries
            .Where(static entry => entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(2, entries.Length);
        Assert.Contains(entries, static entry =>
            entry.Message.Contains("orders", StringComparison.Ordinal) &&
            entry.Message.Contains("orders-consumer", StringComparison.Ordinal) &&
            entry.Message.Contains("orders: submitted", StringComparison.Ordinal));
        Assert.Contains(entries, static entry =>
            entry.Message.Contains("billing", StringComparison.Ordinal) &&
            entry.Message.Contains("billing-consumer", StringComparison.Ordinal) &&
            entry.Message.Contains("orders: cancelled", StringComparison.Ordinal));
    }

    [Fact]
    public async Task SubscriptionSummary_IsOmittedWhenLoggingIsDisabled()
    {
        var services = new ServiceCollection();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        var eventBusBuilder = services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer");
        eventBusBuilder.ConfigureLogging(options => options.Enabled = false);
        eventBusBuilder.AddHandler<RemotingTestHandler>();

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();
        var summary = Assert.Single(
            provider.GetServices<IHostedService>().OfType<RemotingEventBusSubscriptionSummaryHostedService>());

        await summary.StartAsync(TestContext.Current.CancellationToken);

        Assert.DoesNotContain(loggerProvider.Entries, static entry =>
            entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal) &&
            entry.Message.Contains("subscriptions materialized", StringComparison.Ordinal));
    }
}
