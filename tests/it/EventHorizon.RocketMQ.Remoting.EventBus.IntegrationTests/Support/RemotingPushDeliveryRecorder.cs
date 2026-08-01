namespace EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.Support;

internal sealed class RemotingPushDeliveryRecorder(
    IReadOnlyCollection<Guid> expectedTaggedIds,
    IReadOnlyCollection<Guid> expectedUntaggedIds)
{
    private readonly object _syncRoot = new();
    private readonly HashSet<Guid> _expectedTaggedIds = [.. expectedTaggedIds];
    private readonly HashSet<Guid> _expectedUntaggedIds = [.. expectedUntaggedIds];
    private readonly Dictionary<Guid, int> _taggedDeliveries = [];
    private readonly Dictionary<Guid, int> _untaggedDeliveries = [];
    private readonly TaskCompletionSource _allExpectedDeliveries =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal void RecordTagged(Guid deliveryId) => Record(deliveryId, _taggedDeliveries, _expectedTaggedIds, _expectedUntaggedIds);

    internal void RecordUntagged(Guid deliveryId) => Record(deliveryId, _untaggedDeliveries, _expectedTaggedIds, _expectedUntaggedIds);

    internal Task WaitForExpectedDeliveriesAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
        _allExpectedDeliveries.Task.WaitAsync(timeout, cancellationToken);

    internal int GetTaggedDeliveryCount(Guid deliveryId) => GetDeliveryCount(deliveryId, _taggedDeliveries);

    internal int GetUntaggedDeliveryCount(Guid deliveryId) => GetDeliveryCount(deliveryId, _untaggedDeliveries);

    private void Record(
        Guid deliveryId,
        Dictionary<Guid, int> deliveries,
        IReadOnlySet<Guid> expectedTaggedIds,
        IReadOnlySet<Guid> expectedUntaggedIds)
    {
        lock (_syncRoot)
        {
            deliveries.TryGetValue(deliveryId, out var count);
            deliveries[deliveryId] = count + 1;
            if (expectedTaggedIds.All(_taggedDeliveries.ContainsKey) &&
                expectedUntaggedIds.All(_untaggedDeliveries.ContainsKey))
            {
                _allExpectedDeliveries.TrySetResult();
            }
        }
    }

    private int GetDeliveryCount(Guid deliveryId, IReadOnlyDictionary<Guid, int> deliveries)
    {
        lock (_syncRoot)
        {
            return deliveries.GetValueOrDefault(deliveryId);
        }
    }
}
