namespace EventHorizon.RocketMQ.EventBus.Internal.Routing;

/// <summary>
/// Represents one deterministic tag subscription generated for an EventBus registration.
/// </summary>
internal sealed class EventBusSubscription(string topic, string filterExpression)
{
    /// <summary>
    /// Gets the RocketMQ topic.
    /// </summary>
    internal string Topic { get; } = topic;

    /// <summary>
    /// Gets the deterministic literal-tag filter expression.
    /// </summary>
    internal string FilterExpression { get; } = filterExpression;
}
