using EventHorizon.RocketMQ.EventBus.Abstractions;

namespace EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.Support;

internal sealed class RemotingTaggedPushHandler(RemotingPushDeliveryRecorder recorder)
    : IIntegrationEventBusHandler<RemotingTaggedIntegrationEvent>
{
    public Task HandleAsync(RemotingTaggedIntegrationEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.RecordTagged(integrationEvent.DeliveryId);
        return Task.CompletedTask;
    }
}

internal sealed class RemotingUntaggedPushHandler(RemotingPushDeliveryRecorder recorder)
    : IIntegrationEventBusHandler<RemotingUntaggedIntegrationEvent>
{
    public Task HandleAsync(RemotingUntaggedIntegrationEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.RecordUntagged(integrationEvent.DeliveryId);
        return Task.CompletedTask;
    }
}
