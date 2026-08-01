namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Consumer;

internal sealed class RemotingEventBusSubscriptionSummary(EventBusRegistration registration)
{
    private readonly EventBusRegistration _registration = registration ?? throw new ArgumentNullException(nameof(registration));
    private readonly object _sync = new();
    private string? _consumerGroup;
    private int _handlerCount;
    private string[] _subscriptions = [];
    private int _written;

    internal void Materialize(string consumerGroup, IEventBusRoutePlan routePlan)
    {
        ArgumentNullException.ThrowIfNull(consumerGroup);
        ArgumentNullException.ThrowIfNull(routePlan);

        var subscriptions = routePlan.Subscriptions
            .Select(static subscription => $"{subscription.Topic}: {subscription.FilterExpression}")
            .ToArray();

        lock (_sync)
        {
            _consumerGroup = consumerGroup;
            _handlerCount = routePlan.HandlerCount;
            _subscriptions = subscriptions;
        }
    }

    internal void Write(ILogger logger, EventBusLoggingSettings loggingSettings)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(loggingSettings);

        string? consumerGroup;
        int handlerCount;
        string[] subscriptions;
        lock (_sync)
        {
            consumerGroup = _consumerGroup;
            handlerCount = _handlerCount;
            subscriptions = _subscriptions;
        }

        if (consumerGroup is null)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _written, 1, 0) != 0)
        {
            return;
        }

        logger.LogEventBusSubscriptionsMaterialized(
            loggingSettings,
            _registration.RegistrationName ?? "<default>",
            consumerGroup,
            handlerCount,
            subscriptions);
    }
}
