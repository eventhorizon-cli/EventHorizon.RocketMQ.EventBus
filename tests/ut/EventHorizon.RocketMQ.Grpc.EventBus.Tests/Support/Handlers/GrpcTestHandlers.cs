namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Support.Handlers;

internal sealed class OrderCreatedHandler : IIntegrationEventBusHandler<OrderCreatedEvent>
{
    public Task HandleAsync(OrderCreatedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class OrderPlacedHandler : IIntegrationEventBusHandler<OrderPlacedEvent>
{
    public Task HandleAsync(OrderPlacedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class BillingCapturedHandler : IIntegrationEventBusHandler<BillingCapturedEvent>
{
    public Task HandleAsync(BillingCapturedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class GrpcUntaggedHandler(GrpcDispatchRecorder recorder)
    : IIntegrationEventBusHandler<GrpcUntaggedEvent>
{
    public Task HandleAsync(GrpcUntaggedEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Record(integrationEvent.Value ?? string.Empty);
        return Task.CompletedTask;
    }
}

internal sealed class GrpcDispatchHandler(GrpcDispatchRecorder recorder)
    : IIntegrationEventBusHandler<GrpcDispatchEvent>
{
    public Task HandleAsync(GrpcDispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Record(integrationEvent.Value ?? string.Empty);
        return Task.CompletedTask;
    }
}

internal sealed class ThrowingGrpcDispatchHandler : IIntegrationEventBusHandler<GrpcDispatchEvent>
{
    public Task HandleAsync(GrpcDispatchEvent integrationEvent, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Expected Handler failure.");
}

internal sealed class CancellingGrpcDispatchHandler : IIntegrationEventBusHandler<GrpcDispatchEvent>
{
    public Task HandleAsync(GrpcDispatchEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.FromCanceled(cancellationToken);
}
