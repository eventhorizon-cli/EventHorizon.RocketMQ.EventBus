using EventHorizon.RocketMQ.EventBus.Abstractions;

namespace EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests.Support;

internal sealed class GrpcTaggedPushHandler(GrpcPushDeliveryRecorder recorder)
    : IIntegrationEventBusHandler<GrpcTaggedIntegrationEvent>
{
    public Task HandleAsync(GrpcTaggedIntegrationEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.RecordTagged(integrationEvent.DeliveryId);
        return Task.CompletedTask;
    }
}

internal sealed class GrpcUntaggedPushHandler(GrpcPushDeliveryRecorder recorder)
    : IIntegrationEventBusHandler<GrpcUntaggedIntegrationEvent>
{
    public Task HandleAsync(GrpcUntaggedIntegrationEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.RecordUntagged(integrationEvent.DeliveryId);
        return Task.CompletedTask;
    }
}
