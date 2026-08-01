using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Events;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Handlers;

internal sealed class InventorySnapshotHandler(ILogger<InventorySnapshotHandler> logger)
    : IIntegrationEventBusHandler<InventorySnapshotIntegrationEvent>
{
    public Task HandleAsync(
        InventorySnapshotIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation(
            "Handled untagged inventory snapshot for {Warehouse} at {RecordedAt}",
            integrationEvent.Warehouse,
            integrationEvent.RecordedAt);
        return Task.CompletedTask;
    }
}
