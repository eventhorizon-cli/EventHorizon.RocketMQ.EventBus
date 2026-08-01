namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Producer;

public sealed class GrpcIntegrationEventBusTests
{
    [Fact]
    public async Task PublishAsync_SerializesAndSendsOneTaggedMessage()
    {
        var services = CreatePublisherServices(out var producer, out var loggerProvider);
        using (loggerProvider)
        {
            producer
                .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GrpcSendReceiptFactory.Create());

            await using var provider = services.BuildServiceProvider();
            var eventBus = provider.GetRequiredService<IEventBus>();

            await eventBus.PublishAsync(
                new OrderCreatedEvent { Value = "published-json" },
                TestContext.Current.CancellationToken);

            producer.Verify(value => value.SendAsync(
                It.Is<Message>(message => message.Topic == "orders" && message.Tag == "created"),
                TestContext.Current.CancellationToken), Times.Once);
            var entry = Assert.Single(
                loggerProvider.Entries,
                static entry => entry.LogLevel == LogLevel.Information &&
                    entry.Category.StartsWith("EventHorizon.RocketMQ.Grpc.EventBus", StringComparison.Ordinal));
            Assert.Contains("Payload", entry.Message, StringComparison.Ordinal);
            Assert.Contains("{\"Value\":\"published-json\"}", entry.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("EventType", entry.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("RegistrationName", entry.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("HandlerCount", entry.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("Protocol", entry.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task PublishAsync_SucceedsWhenTheLoggerProviderThrows()
    {
        var services = new ServiceCollection();
        using var loggerProvider = new ThrowingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        var producer = CreateProducerMock();
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(GrpcSendReceiptFactory.Create());
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(configureProducer: static _ => { });
        ReplaceProducer(services, producer.Object);

        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredService<IEventBus>().PublishAsync(
            new OrderCreatedEvent { Value = "published" },
            TestContext.Current.CancellationToken);

        producer.Verify(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_SendsAnUntaggedMessageWithoutSettingATransportTag()
    {
        var services = CreatePublisherServices(out var producer);
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(GrpcSendReceiptFactory.Create());

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredService<IEventBus>();

        await eventBus.PublishAsync(new GrpcUntaggedEvent(), TestContext.Current.CancellationToken);

        producer.Verify(value => value.SendAsync(
            It.Is<Message>(message => message.Topic == "orders" && message.Tag == null),
            TestContext.Current.CancellationToken), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_LogsTheDefaultJsonViewWhenUsingACustomSerializer()
    {
        var services = new ServiceCollection();
        using var loggerProvider = new RecordingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(loggerProvider));
        var producer = CreateProducerMock();
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(GrpcSendReceiptFactory.Create());
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(configureProducer: static _ => { });
        ReplaceProducer(services, producer.Object);
        eventBusBuilder.UseSerializer<GrpcBinarySerializer>();

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredService<IEventBus>();

        await eventBus.PublishAsync(
            new OrderCreatedEvent { Value = "custom-published" },
            TestContext.Current.CancellationToken);

        producer.Verify(value => value.SendAsync(
            It.Is<Message>(message => message.Body.SequenceEqual(GrpcBinarySerializer.WirePayload)),
            TestContext.Current.CancellationToken), Times.Once);
        Assert.Contains(
            loggerProvider.Entries,
            static entry => entry.LogLevel == LogLevel.Information &&
                entry.Message.Contains("{\"Value\":\"custom-published\"}", StringComparison.Ordinal));
    }

    [Fact]
    public async Task PublishAsync_WrapsTransportFailures()
    {
        var services = CreatePublisherServices(out var producer);
        var transportException = new InvalidOperationException("Expected transport failure.");
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(transportException);

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredService<IEventBus>();

        var exception = await Assert.ThrowsAsync<EventBusPublishException>(() =>
            eventBus.PublishAsync(new OrderCreatedEvent(), TestContext.Current.CancellationToken));

        Assert.Same(transportException, exception.InnerException);
    }

    [Fact]
    public async Task PublishAsync_OmitsThePayloadWhenPayloadLoggingIsDisabled()
    {
        var services = CreatePublisherServices(out var producer, out var loggerProvider, includePayload: false);
        using (loggerProvider)
        {
            producer
                .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GrpcSendReceiptFactory.Create());

            await using var provider = services.BuildServiceProvider();
            var eventBus = provider.GetRequiredService<IEventBus>();

            await eventBus.PublishAsync(
                new OrderCreatedEvent { Value = "not-logged" },
                TestContext.Current.CancellationToken);

            Assert.Contains(loggerProvider.Entries, static entry => entry.LogLevel == LogLevel.Information);
            Assert.DoesNotContain(
                loggerProvider.Entries,
                static entry => entry.Message.Contains("Payload", StringComparison.Ordinal) ||
                    entry.Message.Contains("not-logged", StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task PublishAsync_EmitsNoEventBusLogsWhenLoggingIsDisabled()
    {
        var services = CreatePublisherServices(out var producer, out var loggerProvider, enabled: false);
        using (loggerProvider)
        {
            producer
                .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GrpcSendReceiptFactory.Create());

            await using var provider = services.BuildServiceProvider();
            await provider.GetRequiredService<IEventBus>().PublishAsync(
                new OrderCreatedEvent { Value = "not-logged" },
                TestContext.Current.CancellationToken);

            Assert.DoesNotContain(loggerProvider.Entries, static entry =>
                entry.Category.StartsWith("EventHorizon.RocketMQ.Grpc.EventBus", StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task PublishAsync_WrapsSerializationFailuresWithoutSending()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var producer = CreateProducerMock();
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(configureProducer: static _ => { });
        ReplaceProducer(services, producer.Object);
        eventBusBuilder.UseSerializer<ThrowingGrpcSerializer>();

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredService<IEventBus>();

        var exception = await Assert.ThrowsAsync<EventBusPublishException>(() =>
            eventBus.PublishAsync(new OrderCreatedEvent(), TestContext.Current.CancellationToken));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        producer.Verify(
            value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task PublishAsync_PreservesCallerRequestedCancellation()
    {
        var services = CreatePublisherServices(out var producer);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), cancellationSource.Token))
            .Returns(Task.FromCanceled<GrpcSendReceipt>(cancellationSource.Token));

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredService<IEventBus>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            eventBus.PublishAsync(new OrderCreatedEvent(), cancellationSource.Token));
    }

    private static ServiceCollection CreatePublisherServices(out Mock<IGrpcProducer> producer)
        => CreatePublisherServices(out producer, out _);

    private static ServiceCollection CreatePublisherServices(
        out Mock<IGrpcProducer> producer,
        out RecordingLoggerProvider loggerProvider,
        bool includePayload = true,
        bool enabled = true)
    {
        var services = new ServiceCollection();
        var provider = new RecordingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(provider));
        loggerProvider = provider;
        producer = CreateProducerMock();
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(configureProducer: static _ => { });
        eventBusBuilder.ConfigureLogging(options =>
        {
            options.Enabled = enabled;
            options.IncludePayload = includePayload;
        });
        ReplaceProducer(services, producer.Object);
        return services;
    }

    private static Mock<IGrpcProducer> CreateProducerMock()
    {
        var producer = new Mock<IGrpcProducer>(MockBehavior.Strict);
        producer.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return producer;
    }

    private static void ReplaceProducer(ServiceCollection services, IGrpcProducer producer)
    {
        for (var index = services.Count - 1; index >= 0; index--)
        {
            var descriptor = services[index];
            if (descriptor.ServiceType == typeof(IGrpcProducer) && !descriptor.IsKeyedService)
            {
                services.RemoveAt(index);
            }
        }

        services.AddSingleton(producer);
    }

    private static void ConfigureClient(GrpcClientOptions options) =>
        options.Endpoint = "http://127.0.0.1:8081";
}
