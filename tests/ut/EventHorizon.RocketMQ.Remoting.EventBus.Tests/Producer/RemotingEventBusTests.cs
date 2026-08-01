using EventHorizon.RocketMQ.EventBus.Exceptions;
using Moq;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Producer;

public sealed class RemotingEventBusTests
{
    [Fact]
    public async Task PublishAsync_SerializesAndSendsOneTaggedMessage()
    {
        var services = CreatePublisherServices(out var producer, out var loggerProvider);
        using (loggerProvider)
        {
            producer
                .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(RemotingTestMessageFactory.CreateSendResult(RemotingSendStatus.SendOk));

            await using var provider = services.BuildServiceProvider();
            var eventBus = provider.GetRequiredService<IEventBus>();
            var integrationEvent = new RemotingTestEvent { Value = "published" };

            await eventBus.PublishAsync(integrationEvent, TestContext.Current.CancellationToken);

            producer.Verify(value => value.SendAsync(
                It.Is<Message>(message =>
                    message.Topic == "orders" &&
                    message.Tag == "submitted" &&
                    System.Text.Encoding.UTF8.GetString(message.Body).Contains("published", StringComparison.Ordinal)),
                TestContext.Current.CancellationToken), Times.Once);
            var entry = Assert.Single(
                loggerProvider.Entries,
                static entry => entry.Level == LogLevel.Information &&
                    entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal));
            Assert.Contains("Payload", entry.Message, StringComparison.Ordinal);
            Assert.Contains("{\"Value\":\"published\"}", entry.Message, StringComparison.Ordinal);
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
        var producer = new Mock<IRemotingProducer>(MockBehavior.Strict);
        producer.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(RemotingTestMessageFactory.CreateSendResult(RemotingSendStatus.SendOk));
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(configureProducer: options => options.GroupName = "eventbus-publisher");
        ReplaceProducer(services, producer.Object, registrationName: null);

        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredService<IEventBus>().PublishAsync(
            new RemotingTestEvent { Value = "published" },
            TestContext.Current.CancellationToken);

        producer.Verify(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WrapsANonSuccessRemotingStatusAndLogsAnError()
    {
        var services = CreatePublisherServices(out var producer, out var loggerProvider);
        using (loggerProvider)
        {
            producer
                .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(RemotingTestMessageFactory.CreateSendResult(RemotingSendStatus.FlushDiskTimeout));

            await using var provider = services.BuildServiceProvider();
            var eventBus = provider.GetRequiredService<IEventBus>();

            var exception = await Assert.ThrowsAsync<EventBusPublishException>(() =>
                eventBus.PublishAsync(new RemotingTestEvent(), TestContext.Current.CancellationToken));

            Assert.Equal(RemotingSendStatus.FlushDiskTimeout.ToString(), exception.TransportResult);
            Assert.Null(exception.InnerException);
            Assert.Contains(loggerProvider.Entries, static entry => entry.Level == LogLevel.Error);
        }
    }

    [Fact]
    public async Task PublishAsync_LogsTheDefaultJsonViewWhenUsingACustomSerializer()
    {
        var services = CreatePublisherServices(out var producer, out var loggerProvider);
        using (loggerProvider)
        {
            producer
                .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(RemotingTestMessageFactory.CreateSendResult(RemotingSendStatus.SendOk));
            var eventBusBuilder = services
                .AddRocketMQRemoting("custom", options => options.NamesrvAddr = "127.0.0.1:9876")
                .AddRemotingEventBus(configureProducer: options => options.GroupName = "custom-publisher");
            ReplaceProducer(services, producer.Object, "custom");
            eventBusBuilder.UseSerializer<RemotingBinarySerializer>();

            await using var provider = services.BuildServiceProvider();
            var eventBus = provider.GetRequiredKeyedService<IEventBus>("custom");

            await eventBus.PublishAsync(
                new RemotingTestEvent { Value = "custom-published" },
                TestContext.Current.CancellationToken);

            producer.Verify(value => value.SendAsync(
                It.Is<Message>(message => message.Body.SequenceEqual(RemotingBinarySerializer.WirePayload)),
                TestContext.Current.CancellationToken), Times.Once);
            Assert.Contains(
                loggerProvider.Entries,
                static entry => entry.Level == LogLevel.Information &&
                    entry.Message.Contains("{\"Value\":\"custom-published\"}", StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task PublishAsync_SendsAnUntaggedMessageWithoutSettingATransportTag()
    {
        var services = CreatePublisherServices(out var producer);
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(RemotingTestMessageFactory.CreateSendResult(RemotingSendStatus.SendOk));

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredService<IEventBus>();

        await eventBus.PublishAsync(new RemotingUntaggedTestEvent(), TestContext.Current.CancellationToken);

        producer.Verify(value => value.SendAsync(
            It.Is<Message>(message => message.Topic == "orders" && message.Tag == null),
            TestContext.Current.CancellationToken), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_OmitsThePayloadWhenPayloadLoggingIsDisabled()
    {
        var services = CreatePublisherServices(out var producer, out var loggerProvider, includePayload: false);
        using (loggerProvider)
        {
            producer
                .Setup(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(RemotingTestMessageFactory.CreateSendResult(RemotingSendStatus.SendOk));

            await using var provider = services.BuildServiceProvider();
            var eventBus = provider.GetRequiredService<IEventBus>();

            await eventBus.PublishAsync(
                new RemotingTestEvent { Value = "not-logged" },
                TestContext.Current.CancellationToken);

            Assert.Contains(loggerProvider.Entries, static entry => entry.Level == LogLevel.Information);
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
                .ReturnsAsync(RemotingTestMessageFactory.CreateSendResult(RemotingSendStatus.SendOk));

            await using var provider = services.BuildServiceProvider();
            await provider.GetRequiredService<IEventBus>().PublishAsync(
                new RemotingTestEvent { Value = "not-logged" },
                TestContext.Current.CancellationToken);

            Assert.DoesNotContain(loggerProvider.Entries, static entry =>
                entry.CategoryName.StartsWith("EventHorizon.RocketMQ.Remoting.EventBus", StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task PublishAsync_WrapsSerializationFailuresAndDoesNotSend()
    {
        var services = CreatePublisherServices(out var producer);
        var eventBusBuilder = services
            .AddRocketMQRemoting("serializer", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(configureProducer: options => options.GroupName = "serializer-publisher");
        ReplaceProducer(services, producer.Object, "serializer");
        eventBusBuilder.UseSerializer<ThrowingSerializer>();

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredKeyedService<IEventBus>("serializer");

        var exception = await Assert.ThrowsAsync<EventBusPublishException>(() =>
            eventBus.PublishAsync(new RemotingTestEvent(), TestContext.Current.CancellationToken));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        producer.Verify(value => value.SendAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task PublishAsync_PreservesCallerRequestedCancellation()
    {
        var services = CreatePublisherServices(out var producer);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();
        producer
            .Setup(value => value.SendAsync(It.IsAny<Message>(), cancellationSource.Token))
            .Returns(Task.FromCanceled<RemotingSendResult>(cancellationSource.Token));

        await using var provider = services.BuildServiceProvider();
        var eventBus = provider.GetRequiredService<IEventBus>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            eventBus.PublishAsync(new RemotingTestEvent(), cancellationSource.Token));
    }

    private static ServiceCollection CreatePublisherServices(out Mock<IRemotingProducer> producer) =>
        CreatePublisherServices(out producer, out _);

    private static ServiceCollection CreatePublisherServices(
        out Mock<IRemotingProducer> producer,
        out CollectingLoggerProvider loggerProvider,
        bool includePayload = true,
        bool enabled = true)
    {
        var services = new ServiceCollection();
        producer = new Mock<IRemotingProducer>(MockBehavior.Strict);
        producer.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var provider = new CollectingLoggerProvider();
        services.AddLogging(logging => logging.AddProvider(provider));
        loggerProvider = provider;
        var eventBusBuilder = services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(configureProducer: options => options.GroupName = "eventbus-publisher");
        eventBusBuilder.ConfigureLogging(options =>
        {
            options.Enabled = enabled;
            options.IncludePayload = includePayload;
        });
        ReplaceProducer(services, producer.Object, null);
        return services;
    }

    private static void ReplaceProducer(ServiceCollection services, IRemotingProducer producer, string? registrationName)
    {
        for (var index = services.Count - 1; index >= 0; index--)
        {
            var descriptor = services[index];
            if (descriptor.ServiceType == typeof(IRemotingProducer) &&
                Equals(descriptor.ServiceKey, registrationName))
            {
                services.RemoveAt(index);
            }
        }

        if (registrationName is null)
        {
            services.AddSingleton(producer);
            return;
        }

        services.AddKeyedSingleton<IRemotingProducer>(registrationName, producer);
    }
}
