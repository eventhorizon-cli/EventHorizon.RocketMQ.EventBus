namespace EventHorizon.RocketMQ.EventBus.Internal.Routing;

internal readonly struct EventBusRouteKey : IEquatable<EventBusRouteKey>
{
    internal EventBusRouteKey(string topic, string? tag)
    {
        ArgumentNullException.ThrowIfNull(topic);
        Topic = topic;
        Tag = tag;
    }

    internal string Topic { get; }

    internal string? Tag { get; }

    public bool Equals(EventBusRouteKey other) =>
        StringComparer.Ordinal.Equals(Topic, other.Topic) && StringComparer.Ordinal.Equals(Tag, other.Tag);

    public override bool Equals(object? obj) => obj is EventBusRouteKey other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(
        Topic is null ? 0 : StringComparer.Ordinal.GetHashCode(Topic),
        Tag is null ? 0 : StringComparer.Ordinal.GetHashCode(Tag));

    public static bool operator ==(EventBusRouteKey left, EventBusRouteKey right) => left.Equals(right);

    public static bool operator !=(EventBusRouteKey left, EventBusRouteKey right) => !left.Equals(right);
}
