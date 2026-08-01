namespace EventHorizon.RocketMQ.EventBus.Samples.Grpc.Publisher.Requests;

public sealed class InventorySnapshotRequest
{
    public string? Warehouse { get; init; }
}
