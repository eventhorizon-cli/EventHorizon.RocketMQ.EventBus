using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;

namespace EventHorizon.RocketMQ.EventBus.Internal.Routing;

internal sealed class EventBusRoutePlan : IEventBusRoutePlan
{
    private readonly IReadOnlyDictionary<EventBusRouteKey, EventBusRoute> _routes;

    private EventBusRoutePlan(
        IReadOnlyDictionary<EventBusRouteKey, EventBusRoute> routes,
        IReadOnlyList<EventBusSubscription> subscriptions,
        int handlerCount)
    {
        _routes = routes;
        Subscriptions = subscriptions;
        HandlerCount = handlerCount;
    }

    public IReadOnlyList<EventBusSubscription> Subscriptions { get; }

    public int HandlerCount { get; }

    public bool TryGetRoute(string? topic, string? tag, [NotNullWhen(true)] out EventBusRoute? route)
    {
        if (topic is null)
        {
            route = null;
            return false;
        }

        return _routes.TryGetValue(new EventBusRouteKey(topic, tag), out route);
    }

    internal static EventBusRoutePlan Create(IEnumerable<EventBusHandlerRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);

        var routeBuilders = new Dictionary<EventBusRouteKey, RouteBuilder>();
        var handlerCount = 0;
        foreach (var registration in registrations)
        {
            handlerCount++;
            if (!routeBuilders.TryGetValue(registration.Route.Key, out var builder))
            {
                builder = new RouteBuilder(registration.Route);
                routeBuilders.Add(registration.Route.Key, builder);
            }

            if (builder.Definition.IntegrationEventType != registration.IntegrationEventType)
            {
                throw new InvalidOperationException(
                    $"Route '{registration.Route.Topic}' and '{registration.Route.Tag}' maps to more than one integration event type.");
            }

            builder.Handlers.Add(registration);
        }

        var routes = new Dictionary<EventBusRouteKey, EventBusRoute>(routeBuilders.Count);
        foreach (var pair in routeBuilders)
        {
            routes.Add(pair.Key, new EventBusRoute(pair.Value.Definition, pair.Value.Handlers));
        }

        var subscriptions = routes.Values
            .GroupBy(static route => route.Definition.Topic, StringComparer.Ordinal)
            .OrderBy(static group => group.Key, StringComparer.Ordinal)
            .Select(static group => new EventBusSubscription(group.Key, CreateFilterExpression(group)))
            .ToArray();

        return new EventBusRoutePlan(
            new ReadOnlyDictionary<EventBusRouteKey, EventBusRoute>(routes),
            new ReadOnlyCollection<EventBusSubscription>(subscriptions),
            handlerCount);
    }

    private static string CreateFilterExpression(IEnumerable<EventBusRoute> routes)
    {
        var tags = routes.Select(static route => route.Definition.Tag).ToArray();
        if (tags.Any(static tag => tag is null))
        {
            return "*";
        }

        return string.Join(
            " || ",
            tags.Select(static tag => tag!).Distinct(StringComparer.Ordinal).OrderBy(static tag => tag, StringComparer.Ordinal));
    }

    private sealed class RouteBuilder(EventBusRouteDefinition definition)
    {
        internal EventBusRouteDefinition Definition { get; } = definition;

        internal List<EventBusHandlerRegistration> Handlers { get; } = [];
    }
}
