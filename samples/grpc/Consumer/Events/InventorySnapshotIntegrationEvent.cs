using EventHorizon.RocketMQ.EventBus.Events;

namespace EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Events;

public sealed class InventorySnapshotIntegrationEvent : IntegrationEvent
{
    public InventorySnapshotIntegrationEvent()
        : base("eventbus-inventory-snapshots")
    {
    }

    public string Warehouse { get; init; } = string.Empty;

    public DateTimeOffset RecordedAt { get; init; }
}
