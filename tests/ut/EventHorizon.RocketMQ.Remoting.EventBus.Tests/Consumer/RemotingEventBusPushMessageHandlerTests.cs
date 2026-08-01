namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Consumer;

public sealed class RemotingEventBusPushMessageHandlerTests
{
    [Fact]
    public async Task HandleAsync_UsesTheTransportScopeToDispatchOneTypedEvent()
    {
        var services = CreateServices();
        var recorder = new RemotingDispatchRecorder();
        services.AddSingleton(recorder);
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingTestMessageFactory.Serialize(new RemotingTestEvent { Value = "received" }));

        var result = await handler.HandleAsync([message], new RemotingPushConsumeContext(), TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.Equal(["received"], recorder.Values);
    }

    [Fact]
    public async Task HandleAsync_LogsTheBrokerQueueLocationWithoutNoisyTypeOrRegistrationFields()
    {
        using var loggerProvider = new CollectingLoggerProvider();
        var services = CreateServices();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services.AddSingleton<RemotingDispatchRecorder>();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingTestMessageFactory.Serialize(new RemotingTestEvent()),
            brokerName: "broker-a",
            queueId: 3,
            queueOffset: 42);

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        var entry = Assert.Single(loggerProvider.Entries, static entry =>
            entry.Level == LogLevel.Information &&
            entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal));
        Assert.Contains("BrokerName: broker-a", entry.Message, StringComparison.Ordinal);
        Assert.Contains("QueueId: 3", entry.Message, StringComparison.Ordinal);
        Assert.Contains("QueueOffset: 42", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("EventType", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("RegistrationName", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("HandlerCount", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Protocol", entry.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HandleAsync_ReturnsSuccessWhenTheLoggerProviderThrows()
    {
        using var loggerProvider = new ThrowingLoggerProvider();
        var services = CreateServices();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services.AddSingleton<RemotingDispatchRecorder>();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingTestMessageFactory.Serialize(new RemotingTestEvent()));

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
    }

    [Fact]
    public async Task HandleAsync_ReturnsDeadLetterForAnUnknownRouteAndLogsTheJsonPayload()
    {
        var services = CreateServices();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var payload = RemotingTestMessageFactory.Serialize(new RemotingTestEvent { Value = "do-not-log-this" });
        var message = RemotingTestMessageFactory.Create("unknown", "route", payload);

        var result = await handler.HandleAsync([message], new RemotingPushConsumeContext(), TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.DeadLetter, result);
        var entries = loggerProvider.Entries
            .Where(static entry => entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal))
            .ToArray();
        Assert.Contains(entries, static entry => entry.Level == LogLevel.Error);
        Assert.Contains(
            entries,
            static entry => entry.Level == LogLevel.Error &&
                entry.Message.Contains("Payload", StringComparison.Ordinal) &&
                entry.Message.Contains("{\"Value\":\"do-not-log-this\"}", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HandleAsync_DoesNotLogThePayloadWhenDeserializationFails()
    {
        var services = CreateServices();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create("orders", "submitted", new byte[] { 0xff });

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.DeadLetter, result);
        Assert.Contains(loggerProvider.Entries, static entry => entry.Level == LogLevel.Error);
        Assert.DoesNotContain(
            loggerProvider.Entries,
            static entry => entry.Level == LogLevel.Error && entry.Message.Contains("Payload", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HandleAsync_RoutesAnUntaggedMessageToItsConcreteHandler()
    {
        var services = CreateServices();
        var recorder = new RemotingDispatchRecorder();
        services.AddSingleton(recorder);
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingUntaggedTestHandler>();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler<RemotingUntaggedTestHandler>(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            null,
            RemotingTestMessageFactory.Serialize(new RemotingUntaggedTestEvent { Value = "untagged" }));

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.Equal(["untagged"], recorder.Values);
    }

    [Fact]
    public async Task HandleAsync_ReturnsRetryWhenAnApplicationHandlerFails()
    {
        var services = CreateServices();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<ThrowingRemotingTestHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler<ThrowingRemotingTestHandler>(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingTestMessageFactory.Serialize(new RemotingTestEvent()));

        var result = await handler.HandleAsync([message], new RemotingPushConsumeContext(), TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Retry, result);
    }

    [Fact]
    public async Task HandleAsync_ReturnsRetryAndLogsTheOutcomeWhenTheDispatchBridgeFails()
    {
        using var loggerProvider = new CollectingLoggerProvider();
        var services = CreateServices();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services.AddSingleton<RemotingDispatchRecorder>();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();
        var dispatcherDescriptor = Assert.Single(services, static descriptor =>
            descriptor.ServiceType.FullName == "EventHorizon.RocketMQ.EventBus.Internal.Dispatching.IEventBusDispatchRuntime");
        services.Remove(dispatcherDescriptor);
        ((IServiceCollection)services).Add(ServiceDescriptor.DescribeKeyed(
            dispatcherDescriptor.ServiceType,
            dispatcherDescriptor.ServiceKey,
            static (_, _) => throw new InvalidOperationException("Expected dispatch bridge failure."),
            ServiceLifetime.Scoped));

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingTestMessageFactory.Serialize(new RemotingTestEvent()));

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Retry, result);
        Assert.Contains(loggerProvider.Entries, static entry =>
            entry.Level == LogLevel.Error &&
            entry.Message.Contains("Outcome: Retry", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HandleAsync_LogsTheDefaultJsonViewWhenUsingACustomSerializer()
    {
        var services = CreateServices();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services.AddSingleton<RemotingDispatchRecorder>();
        var eventBusBuilder = services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer");
        eventBusBuilder.AddHandler<RemotingTestHandler>();
        eventBusBuilder.UseSerializer<RemotingBinarySerializer>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingBinarySerializer.WirePayload);

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.Contains(
            loggerProvider.Entries,
            static entry => entry.Level == LogLevel.Information &&
                entry.Message.Contains("{\"Value\":\"custom-consumed\"}", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HandleAsync_OmitsThePayloadWhenPayloadLoggingIsDisabled()
    {
        var services = CreateServices();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services.AddSingleton<RemotingDispatchRecorder>();
        var eventBusBuilder = services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer");
        eventBusBuilder.ConfigureLogging(options => options.IncludePayload = false);
        eventBusBuilder.AddHandler<RemotingTestHandler>();
        var payload = RemotingTestMessageFactory.Serialize(new RemotingTestEvent { Value = "not-logged" });

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create("orders", "submitted", payload);

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.DoesNotContain(
            loggerProvider.Entries,
            static entry => entry.Message.Contains("Payload", StringComparison.Ordinal) ||
                entry.Message.Contains("not-logged", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HandleAsync_EmitsNoEventBusLogsWhenLoggingIsDisabled()
    {
        var services = CreateServices();
        using var loggerProvider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        services.AddSingleton<RemotingDispatchRecorder>();
        var eventBusBuilder = services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer");
        eventBusBuilder.ConfigureLogging(options => options.Enabled = false);
        eventBusBuilder.AddHandler<RemotingTestHandler>();
        var payload = RemotingTestMessageFactory.Serialize(new RemotingTestEvent());

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create("orders", "submitted", payload);

        var result = await handler.HandleAsync(
            [message],
            new RemotingPushConsumeContext(),
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.DoesNotContain(loggerProvider.Entries, static entry =>
            entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HandleAsync_PropagatesTransportCancellation()
    {
        var services = CreateServices();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<CancellingRemotingTestHandler>();
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler<CancellingRemotingTestHandler>(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingTestMessageFactory.Serialize(new RemotingTestEvent()));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await handler.HandleAsync([message], new RemotingPushConsumeContext(), cancellationSource.Token).AsTask());
    }

    [Fact]
    public async Task HandleAsync_ReturnsRetryWhenTheTransportViolatesTheSingleMessageContract()
    {
        var services = CreateServices();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<RemotingTestHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = CreateHandler(scope.ServiceProvider);
        var message = RemotingTestMessageFactory.Create(
            "orders",
            "submitted",
            RemotingTestMessageFactory.Serialize(new RemotingTestEvent()));

        var result = await handler.HandleAsync([message, message], new RemotingPushConsumeContext(), TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Retry, result);
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    private static IRemotingPushMessageHandler CreateHandler(IServiceProvider serviceProvider) =>
        (IRemotingPushMessageHandler)ActivatorUtilities.CreateInstance<RemotingEventBusPushMessageHandler<RemotingTestHandler>>(
            serviceProvider);

    private static IRemotingPushMessageHandler CreateHandler<TAnchorHandler>(IServiceProvider serviceProvider)
        where TAnchorHandler : class =>
        (IRemotingPushMessageHandler)ActivatorUtilities.CreateInstance<RemotingEventBusPushMessageHandler<TAnchorHandler>>(
            serviceProvider);
}
