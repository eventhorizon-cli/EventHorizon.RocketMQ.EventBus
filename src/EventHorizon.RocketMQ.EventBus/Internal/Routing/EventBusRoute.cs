using System.Collections.ObjectModel;

namespace EventHorizon.RocketMQ.EventBus.Internal.Routing;

/// <summary>
/// Represents an immutable application route and its ordered handlers.
/// </summary>
internal sealed class EventBusRoute
{
    internal EventBusRoute(EventBusRouteDefinition definition, IEnumerable<EventBusHandlerRegistration> handlers)
    {
        Definition = definition;
        Handlers = new ReadOnlyCollection<EventBusHandlerRegistration>(handlers.ToArray());
    }

    internal EventBusRouteDefinition Definition { get; }

    internal Type IntegrationEventType => Definition.IntegrationEventType;

    internal IReadOnlyList<EventBusHandlerRegistration> Handlers { get; }
}
