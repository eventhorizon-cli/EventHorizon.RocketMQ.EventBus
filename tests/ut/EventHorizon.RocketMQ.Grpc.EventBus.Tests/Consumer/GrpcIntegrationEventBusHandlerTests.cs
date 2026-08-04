namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Consumer;

public sealed class GrpcIntegrationEventBusHandlerTests
{
    [Fact]
    public async Task DispatchAsync_UsesTheTransportScopeToDispatchOneTypedEvent()
    {
        var services = CreateServices();
        var recorder = new GrpcDispatchRecorder();
        services.AddSingleton(recorder);
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<GrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(
            new GrpcDispatchEvent { Value = "received" });

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            payload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.Equal(["received"], recorder.Values);
    }

    [Fact]
    public async Task DispatchAsync_LogsTheBrokerQueueLocationWithoutNoisyTypeOrRegistrationFields()
    {
        using var logs = new RecordingLoggerProvider();
        var services = CreateServices();
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<GrpcDispatchRecorder>();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<GrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new GrpcDispatchEvent());

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            payload,
            "message-id",
            2,
            TestContext.Current.CancellationToken,
            "broker-a",
            3,
            42);

        Assert.Equal(ConsumeResult.Success, result);
        var entry = Assert.Single(logs.Entries, static entry =>
            entry.LogLevel == LogLevel.Information &&
            entry.Category.StartsWith("EventHorizon.RocketMQ.Grpc.EventBus", StringComparison.Ordinal));
        Assert.Contains("BrokerName: broker-a", entry.Message, StringComparison.Ordinal);
        Assert.Contains("QueueId: 3", entry.Message, StringComparison.Ordinal);
        Assert.Contains("QueueOffset: 42", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("EventType", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("RegistrationName", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("HandlerCount", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Protocol", entry.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsSuccessWhenTheLoggerProviderThrows()
    {
        using var logs = new ThrowingLoggerProvider();
        var services = CreateServices();
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<GrpcDispatchRecorder>();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<GrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new GrpcDispatchEvent());

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            payload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
    }

    [Fact]
    public async Task DispatchAsync_RoutesAnUntaggedMessageToItsConcreteHandler()
    {
        var services = CreateServices();
        var recorder = new GrpcDispatchRecorder();
        services.AddSingleton(recorder);
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "orders-consumer")
            .AddHandler<GrpcUntaggedHandler>();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcUntaggedHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(
            new GrpcUntaggedEvent { Value = "untagged" });

        var result = await handler.DispatchAsync(
            "orders",
            null,
            payload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.Equal(["untagged"], recorder.Values);
    }

    [Fact]
    public async Task DispatchAsync_UnknownRoute_ReturnsFailureAndLogsDeadLetterPayloadAsBase64Json()
    {
        var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddProvider(logs);
        });
        services.AddSingleton<GrpcDispatchRecorder>();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<GrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payloadText = "do-not-log-this";

        var result = await handler.DispatchAsync(
            "unknown",
            "route",
            System.Text.Encoding.UTF8.GetBytes(payloadText),
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Failure, result);
        Assert.Contains(logs.Entries, static entry => entry.LogLevel == LogLevel.Error);
        var expectedBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(payloadText));
        Assert.Contains(
            logs.Entries,
            entry => entry.LogLevel == LogLevel.Error &&
                entry.Message.Contains("Payload", StringComparison.Ordinal) &&
                entry.Message.Contains("{\"encoding\":\"base64\",\"data\":\"" + expectedBase64 + "\"}", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DispatchAsync_DeserializationFails_ReturnsFailureWithoutLoggingPayload()
    {
        using var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<GrpcDispatchRecorder>();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<GrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            new byte[] { 0xff },
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Failure, result);
        Assert.Contains(logs.Entries, static entry => entry.LogLevel == LogLevel.Error);
        Assert.DoesNotContain(
            logs.Entries,
            static entry => entry.LogLevel == LogLevel.Error && entry.Message.Contains("Payload", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DispatchAsync_ApplicationHandlerFails_ReturnsFailure()
    {
        var services = CreateServices();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<ThrowingGrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<ThrowingGrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new GrpcDispatchEvent());

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            payload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Failure, result);
    }

    [Fact]
    public async Task DispatchAsync_DispatchBridgeFails_ReturnsFailureAndLogsRetryOutcome()
    {
        using var logs = new RecordingLoggerProvider();
        var services = CreateServices();
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<GrpcDispatchRecorder>();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<GrpcDispatchHandler>();
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
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new GrpcDispatchEvent());

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            payload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Failure, result);
        Assert.Contains(logs.Entries, static entry =>
            entry.LogLevel == LogLevel.Error &&
            entry.Message.Contains("Outcome: Retry", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DispatchAsync_LogsTheDefaultJsonViewWhenUsingACustomSerializer()
    {
        using var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<GrpcDispatchRecorder>();
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer");
        eventBusBuilder.AddHandler<GrpcDispatchHandler>();
        eventBusBuilder.UseSerializer<GrpcBinarySerializer>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            GrpcBinarySerializer.WirePayload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.Contains(
            logs.Entries,
            static entry => entry.LogLevel == LogLevel.Information &&
                entry.Message.Contains("{\"Value\":\"custom-consumed\"}", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DispatchAsync_OmitsThePayloadWhenPayloadLoggingIsDisabled()
    {
        using var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<GrpcDispatchRecorder>();
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer");
        eventBusBuilder.ConfigureLogging(options => options.IncludePayload = false);
        eventBusBuilder.AddHandler<GrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(
            new GrpcDispatchEvent { Value = "not-logged" });

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            payload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.DoesNotContain(
            logs.Entries,
            static entry => entry.Message.Contains("Payload", StringComparison.Ordinal) ||
                entry.Message.Contains("not-logged", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DispatchAsync_EmitsNoEventBusLogsWhenLoggingIsDisabled()
    {
        using var logs = new RecordingLoggerProvider();
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<GrpcDispatchRecorder>();
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer");
        eventBusBuilder.ConfigureLogging(options => options.Enabled = false);
        eventBusBuilder.AddHandler<GrpcDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<GrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new GrpcDispatchEvent());

        var result = await handler.DispatchAsync(
            "dispatch",
            "received",
            payload,
            "message-id",
            1,
            TestContext.Current.CancellationToken);

        Assert.Equal(ConsumeResult.Success, result);
        Assert.DoesNotContain(logs.Entries, static entry =>
            entry.Category.StartsWith("EventHorizon.RocketMQ.Grpc.EventBus", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DispatchAsync_PropagatesTransportCancellation()
    {
        var services = CreateServices();
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options => options.GroupName = "dispatch-consumer")
            .AddHandler<CancellingGrpcDispatchHandler>();
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var handler = ActivatorUtilities.CreateInstance<GrpcIntegrationEventBusHandler<CancellingGrpcDispatchHandler>>(
            scope.ServiceProvider);
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new GrpcDispatchEvent());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await handler.DispatchAsync(
                "dispatch",
                "received",
                payload,
                "message-id",
                1,
                cancellationSource.Token).AsTask());
    }

    private static ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    private static void ConfigureClient(GrpcClientOptions options) =>
        options.Endpoint = "http://127.0.0.1:8081";
}
