namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Registration;

internal sealed class GrpcEventBusConsumerConfiguration
{
    private readonly object _syncRoot = new();
    private GrpcEventBusConsumerConfigurationSnapshot? _snapshot;

    internal void Set(string groupName, IEventBusRoutePlan routePlan)
    {
        ArgumentNullException.ThrowIfNull(groupName);
        ArgumentNullException.ThrowIfNull(routePlan);

        var subscriptions = routePlan.Subscriptions
            .Select(static subscription => new GrpcEventBusSubscription(
                subscription.Topic,
                subscription.FilterExpression))
            .ToArray();

        lock (_syncRoot)
        {
            _snapshot = new GrpcEventBusConsumerConfigurationSnapshot(groupName, routePlan.HandlerCount, subscriptions);
        }
    }

    internal GrpcEventBusConsumerConfigurationSnapshot GetRequiredSnapshot()
    {
        lock (_syncRoot)
        {
            return _snapshot ?? throw new InvalidOperationException(
                "The gRPC EventBus Push consumer options were not materialized before the subscription summary started.");
        }
    }
}
