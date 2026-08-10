using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.EventBus.Internal.Dispatching;

internal sealed class EventBusDispatchRuntime(
    object registrationToken,
    IEventBusRoutePlan routePlan,
    IIntegrationEventSerializer serializer,
    EventBusConsumptionSettings consumptionSettings,
    IServiceProvider serviceProvider) : IEventBusDispatchRuntime
{
    private readonly object _registrationToken = registrationToken ?? throw new ArgumentNullException(nameof(registrationToken));
    private readonly IEventBusRoutePlan _routePlan = routePlan ?? throw new ArgumentNullException(nameof(routePlan));
    private readonly IIntegrationEventSerializer _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly EventBusConsumptionSettings _consumptionSettings =
        consumptionSettings ?? throw new ArgumentNullException(nameof(consumptionSettings));
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));

    public ValueTask<EventBusDispatchResult> DispatchAsync(
        string? topic,
        string? tag,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        if (!_routePlan.TryGetRoute(topic, tag, out var route))
        {
            return ValueTask.FromResult(EventBusDispatchResult.Retry(null, 0));
        }

        return DispatchRouteAsync(route, payload, cancellationToken);
    }

    public ValueTask<EventBusDispatchResult> DispatchAsync(
        Type integrationEventType,
        string? topic,
        string? tag,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(integrationEventType);

        if (!_routePlan.TryGetRoute(topic, tag, out var route) || route.IntegrationEventType != integrationEventType)
        {
            return ValueTask.FromResult(EventBusDispatchResult.Retry(integrationEventType, 0));
        }

        return DispatchRouteAsync(route, payload, cancellationToken);
    }

    private async ValueTask<EventBusDispatchResult> DispatchRouteAsync(
        EventBusRoute route,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        if (route.Handlers.Count == 0)
        {
            return EventBusDispatchResult.Retry(route.IntegrationEventType, 0);
        }

        IntegrationEvent integrationEvent;
        try
        {
            integrationEvent = _serializer.Deserialize(payload, route.IntegrationEventType);
            if (integrationEvent is null || integrationEvent.GetType() != route.IntegrationEventType)
            {
                return EventBusDispatchResult.DeserializationFailure(
                    route.IntegrationEventType,
                    route.Handlers.Count,
                    _consumptionSettings.SkipDeserializationFailures);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return EventBusDispatchResult.DeserializationFailure(
                route.IntegrationEventType,
                route.Handlers.Count,
                _consumptionSettings.SkipDeserializationFailures);
        }

        foreach (var handlerRegistration in route.Handlers)
        {
            try
            {
                var handler = _serviceProvider.GetRequiredKeyedService(handlerRegistration.HandlerType, _registrationToken);
                await handlerRegistration.InvokeAsync(handler, integrationEvent, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                return EventBusDispatchResult.Retry(integrationEvent, route.Handlers.Count, exception);
            }
        }

        return EventBusDispatchResult.Success(integrationEvent, route.Handlers.Count);
    }
}
