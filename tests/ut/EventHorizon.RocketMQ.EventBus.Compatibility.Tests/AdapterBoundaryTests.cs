using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Events;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.EventBus;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.EventBus;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using GrpcConsumeResult = EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult;
using RemotingConsumeResult = EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult;

namespace EventHorizon.RocketMQ.EventBus.Compatibility.Tests;

public sealed class AdapterBoundaryTests
{
    [Fact]
    public void AddEventBus_RejectsTheSameNamedIdentityAcrossProtocols()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc("orders", options => options.Endpoint = "http://127.0.0.1:8081")
            .AddGrpcEventBus();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services
                .AddRocketMQRemoting("orders", options => options.NamesrvAddr = "127.0.0.1:9876")
                .AddRemotingEventBus());

        Assert.Contains("orders", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddHandler_RejectsTheSameConcreteHandlerAcrossProtocols()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc("grpc", options => options.Endpoint = "http://127.0.0.1:8081")
            .AddGrpcEventBus(options => options.GroupName = "grpc-orders")
            .AddHandler<SharedOrderHandler>();

        var remoting = services
            .AddRocketMQRemoting("remoting", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "remoting-orders");

        var exception = Assert.Throws<InvalidOperationException>(() =>
            remoting.AddHandler<SharedOrderHandler>());

        Assert.Contains(typeof(SharedOrderHandler).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains("grpc", exception.Message, StringComparison.Ordinal);
        Assert.Contains("remoting", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddEventBus_PublicExtensionShapesRemainSymmetric()
    {
        var grpc = typeof(GrpcEventBusBuilderExtensions).GetMethod(nameof(GrpcEventBusBuilderExtensions.AddGrpcEventBus))!;
        var remoting = typeof(RemotingEventBusBuilderExtensions).GetMethod(
            nameof(RemotingEventBusBuilderExtensions.AddRemotingEventBus))!;

        Assert.Equal(typeof(IEventBusBuilder), grpc.ReturnType);
        Assert.Equal(typeof(IEventBusBuilder), remoting.ReturnType);
        Assert.Equal(
            ["builder", "configureConsumer", "configureProducer"],
            grpc.GetParameters().Select(static parameter => parameter.Name));
        Assert.Equal(
            ["builder", "configureConsumer", "configureProducer"],
            remoting.GetParameters().Select(static parameter => parameter.Name));
        Assert.All(grpc.GetParameters().Skip(1), static parameter => Assert.True(parameter.HasDefaultValue));
        Assert.All(remoting.GetParameters().Skip(1), static parameter => Assert.True(parameter.HasDefaultValue));
    }

    [Fact]
    public void ConsumeResult_IndependentTransportPackages_ExposeProtocolSpecificValues()
    {
        Assert.NotEqual(typeof(GrpcConsumeResult), typeof(RemotingConsumeResult));
        Assert.Equal(["Success", "Failure"], Enum.GetNames<GrpcConsumeResult>());
        Assert.Equal(["Success", "Retry", "DeadLetter"], Enum.GetNames<RemotingConsumeResult>());
    }

    private sealed class SharedOrderEvent : IntegrationEvent
    {
        public SharedOrderEvent()
            : base("orders", "shared")
        {
        }
    }

    private sealed class SharedOrderHandler : IIntegrationEventBusHandler<SharedOrderEvent>
    {
        public Task HandleAsync(SharedOrderEvent integrationEvent, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
