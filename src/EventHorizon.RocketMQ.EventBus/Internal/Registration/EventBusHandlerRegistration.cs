using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.EventBus.Internal.Registration;

internal sealed class EventBusHandlerRegistration
{
    private static readonly MethodInfo CreateTypedInvokerMethod = typeof(EventBusHandlerRegistration)
        .GetMethod(nameof(CreateTypedInvoker), BindingFlags.NonPublic | BindingFlags.Static)!;

    private EventBusHandlerRegistration(
        Type handlerType,
        EventBusRouteDefinition route,
        ServiceLifetime lifetime,
        Func<object, IntegrationEvent, CancellationToken, Task> invokeAsync)
    {
        HandlerType = handlerType;
        Route = route;
        Lifetime = lifetime;
        InvokeAsync = invokeAsync;
    }

    internal Type HandlerType { get; }

    internal Type IntegrationEventType => Route.IntegrationEventType;

    internal EventBusRouteDefinition Route { get; }

    internal ServiceLifetime Lifetime { get; }

    internal Func<object, IntegrationEvent, CancellationToken, Task> InvokeAsync { get; }

    internal static EventBusHandlerRegistration Create(
        Type handlerType,
        EventBusRouteDefinition route,
        ServiceLifetime lifetime) =>
        new(handlerType, route, lifetime, CreateInvoker(route.IntegrationEventType));

    private static Func<object, IntegrationEvent, CancellationToken, Task> CreateInvoker(Type integrationEventType)
    {
        return (Func<object, IntegrationEvent, CancellationToken, Task>)CreateTypedInvokerMethod
            .MakeGenericMethod(integrationEventType)
            .Invoke(null, null)!;
    }

    private static Func<object, IntegrationEvent, CancellationToken, Task> CreateTypedInvoker<TIntegrationEvent>()
        where TIntegrationEvent : IntegrationEvent =>
        static (handler, integrationEvent, cancellationToken) =>
            ((IIntegrationEventBusHandler<TIntegrationEvent>)handler)
            .HandleAsync((TIntegrationEvent)integrationEvent, cancellationToken);
}
