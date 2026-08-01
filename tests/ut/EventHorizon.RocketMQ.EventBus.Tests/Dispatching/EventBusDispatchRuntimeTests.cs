using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.EventBus.Tests.Dispatching;

public sealed class EventBusDispatchRuntimeTests
{
    [Fact]
    public async Task DispatchAsync_DeserializesOnceAndInvokesAllHandlersSequentially()
    {
        var (services, registration) = CreateRegistration();
        var recorder = new DispatchRecorder();
        services.AddSingleton(recorder);
        registration.Builder
            .AddHandler<DispatchFirstHandler>()
            .AddHandler<DispatchSecondHandler>();
        var serializer = new Mock<IIntegrationEventSerializer>(MockBehavior.Strict);
        serializer
            .Setup(value => value.Deserialize(It.IsAny<ReadOnlyMemory<byte>>(), typeof(DispatchEvent)))
            .Returns(new DispatchEvent { Value = "value" });
        ReplaceSerializer(services, registration, serializer.Object);

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var dispatcher = registration.GetRequiredDispatcher(scope.ServiceProvider);

        var result = await dispatcher.DispatchAsync("dispatch", "received", new byte[] { 1, 2, 3 }, TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.Success, result.Outcome);
        Assert.Equal(typeof(DispatchEvent), result.IntegrationEventType);
        Assert.IsType<DispatchEvent>(result.IntegrationEvent);
        Assert.False(result.DeserializationFailed);
        Assert.Equal(2, result.HandlerCount);
        Assert.Null(result.Exception);
        Assert.Equal(["first", "second"], recorder.Entries);
        serializer.Verify(value => value.Deserialize(It.IsAny<ReadOnlyMemory<byte>>(), typeof(DispatchEvent)), Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsDeadLetterForAnUnknownRoute()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder.AddHandler<DispatchFirstHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var result = await registration.GetRequiredDispatcher(scope.ServiceProvider)
            .DispatchAsync("missing", "route", ReadOnlyMemory<byte>.Empty, TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.DeadLetter, result.Outcome);
        Assert.Null(result.IntegrationEventType);
        Assert.Null(result.IntegrationEvent);
        Assert.Equal(0, result.HandlerCount);
    }

    [Fact]
    public async Task DispatchAsync_RoutesAnUntaggedMessageToItsConcreteEventHandler()
    {
        var (services, registration) = CreateRegistration();
        var recorder = new DispatchRecorder();
        services.AddSingleton(recorder);
        registration.Builder.AddHandler<UntaggedDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new UntaggedDispatchEvent());
        var result = await registration.GetRequiredDispatcher(scope.ServiceProvider)
            .DispatchAsync("dispatch", null, payload, TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.Success, result.Outcome);
        Assert.Equal(typeof(UntaggedDispatchEvent), result.IntegrationEventType);
        Assert.Equal(["untagged"], recorder.Entries);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsDeadLetterForAnInvalidPayloadWithoutInvokingAHandler()
    {
        var (services, registration) = CreateRegistration();
        var recorder = new DispatchRecorder();
        services.AddSingleton(recorder);
        registration.Builder.AddHandler<DispatchFirstHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var result = await registration.GetRequiredDispatcher(scope.ServiceProvider)
            .DispatchAsync("dispatch", "received", new byte[] { 0xff }, TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.DeadLetter, result.Outcome);
        Assert.Equal(typeof(DispatchEvent), result.IntegrationEventType);
        Assert.True(result.DeserializationFailed);
        Assert.Empty(recorder.Entries);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsDeadLetterWhenACustomSerializerReturnsTheWrongType()
    {
        var (services, registration) = CreateRegistration();
        var recorder = new DispatchRecorder();
        services.AddSingleton(recorder);
        registration.Builder.AddHandler<DispatchFirstHandler>();
        var serializer = new Mock<IIntegrationEventSerializer>(MockBehavior.Strict);
        serializer
            .Setup(value => value.Deserialize(It.IsAny<ReadOnlyMemory<byte>>(), typeof(DispatchEvent)))
            .Returns(new OrderSubmittedEvent());
        ReplaceSerializer(services, registration, serializer.Object);

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var result = await registration.GetRequiredDispatcher(scope.ServiceProvider)
            .DispatchAsync("dispatch", "received", ReadOnlyMemory<byte>.Empty, TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.DeadLetter, result.Outcome);
        Assert.Empty(recorder.Entries);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsRetryAndStopsAfterTheFirstHandlerFailure()
    {
        var (services, registration) = CreateRegistration();
        var recorder = new DispatchRecorder();
        services.AddSingleton(recorder);
        registration.Builder
            .AddHandler<ThrowingDispatchHandler>()
            .AddHandler<AfterThrowingDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new DispatchEvent());
        var result = await registration.GetRequiredDispatcher(scope.ServiceProvider)
            .DispatchAsync("dispatch", "received", payload, TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.Retry, result.Outcome);
        Assert.IsType<InvalidOperationException>(result.Exception);
        Assert.Equal(["throwing"], recorder.Entries);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsRetryWhenHandlerDependencyResolutionFails()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder.AddHandler<MissingDependencyDispatchHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new DispatchEvent());
        var result = await registration.GetRequiredDispatcher(scope.ServiceProvider)
            .DispatchAsync("dispatch", "received", payload, TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.Retry, result.Outcome);
        Assert.IsType<InvalidOperationException>(result.Exception);
    }

    [Fact]
    public async Task DispatchAsync_PropagatesCancellationForTheTransportToSettle()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder.AddHandler<CancellingDispatchHandler>();
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new DispatchEvent());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await registration.GetRequiredDispatcher(scope.ServiceProvider)
                .DispatchAsync("dispatch", "received", payload, cancellationSource.Token)
                .AsTask());
    }

    [Fact]
    public async Task DispatchAsync_UsesTheExistingScopeForAllHandlersAndLeavesItsDisposalToTheTransport()
    {
        var (services, registration) = CreateRegistration();
        var recorder = new DispatchRecorder();
        services.AddSingleton(recorder);
        services.AddScoped<ScopedProbe>();
        registration.Builder
            .AddHandler<ScopedDispatchFirstHandler>()
            .AddHandler<ScopedDispatchSecondHandler>();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        ScopedProbe probe;
        await using (var scope = provider.CreateAsyncScope())
        {
            probe = scope.ServiceProvider.GetRequiredService<ScopedProbe>();
            var payload = new NewtonsoftJsonIntegrationEventSerializer().Serialize(new DispatchEvent());
            var result = await registration.GetRequiredDispatcher(scope.ServiceProvider)
                .DispatchAsync("dispatch", "received", payload, TestContext.Current.CancellationToken);

            Assert.Equal(EventBusDispatchOutcome.Success, result.Outcome);
            Assert.Equal(2, recorder.Entries.Count);
            Assert.Equal(probe.InstanceId.ToString(), recorder.Entries[0].Split(':')[1]);
            Assert.Equal(probe.InstanceId.ToString(), recorder.Entries[1].Split(':')[1]);
            Assert.False(probe.Disposed);
        }

        Assert.True(probe.Disposed);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsDeadLetterWhenTheAdapterSuppliesATypeThatDoesNotMatchTheRoute()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder.AddHandler<DispatchFirstHandler>();

        using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var result = await registration.GetRequiredDispatcher(scope.ServiceProvider).DispatchAsync(
            typeof(OrderSubmittedEvent),
            "dispatch",
            "received",
            ReadOnlyMemory<byte>.Empty,
            TestContext.Current.CancellationToken);

        Assert.Equal(EventBusDispatchOutcome.DeadLetter, result.Outcome);
        Assert.Equal(typeof(OrderSubmittedEvent), result.IntegrationEventType);
    }

    private static (ServiceCollection Services, EventBusRegistration Registration) CreateRegistration()
    {
        var services = new ServiceCollection();
        return (services, EventBusRegistration.Create(services, null));
    }

    private static void ReplaceSerializer(
        ServiceCollection services,
        EventBusRegistration registration,
        IIntegrationEventSerializer serializer)
    {
        for (var index = services.Count - 1; index >= 0; index--)
        {
            var descriptor = services[index];
            if (descriptor.ServiceType == typeof(IIntegrationEventSerializer) && ReferenceEquals(descriptor.ServiceKey, registration.Token))
            {
                services.RemoveAt(index);
            }
        }

        services.AddKeyedSingleton<IIntegrationEventSerializer>(registration.Token, serializer);
    }
}
