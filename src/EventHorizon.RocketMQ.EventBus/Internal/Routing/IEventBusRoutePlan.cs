using System.Diagnostics.CodeAnalysis;

namespace EventHorizon.RocketMQ.EventBus.Internal.Routing;

/// <summary>
/// Represents the immutable subscription and route snapshot for one EventBus registration.
/// </summary>
internal interface IEventBusRoutePlan
{
    /// <summary>
    /// Gets the deterministic subscriptions generated from the registration's routes.
    /// </summary>
    IReadOnlyList<EventBusSubscription> Subscriptions { get; }

    /// <summary>
    /// Gets the number of registered event-handler pairs.
    /// </summary>
    int HandlerCount { get; }

    /// <summary>
    /// Resolves a received topic and tag to its immutable application route.
    /// </summary>
    /// <param name="topic">The received topic.</param>
    /// <param name="tag">The received tag.</param>
    /// <param name="route">The matching route, when found.</param>
    /// <returns><see langword="true"/> when a route was found; otherwise, <see langword="false"/>.</returns>
    bool TryGetRoute(string? topic, string? tag, [NotNullWhen(true)] out EventBusRoute? route);
}
