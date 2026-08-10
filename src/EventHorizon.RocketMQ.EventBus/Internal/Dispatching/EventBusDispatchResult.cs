namespace EventHorizon.RocketMQ.EventBus.Internal.Dispatching;

/// <summary>
/// Contains the transport-neutral dispatch result and adapter logging context for one delivery.
/// </summary>
internal readonly record struct EventBusDispatchResult(
    EventBusDispatchOutcome Outcome,
    Type? IntegrationEventType,
    IntegrationEvent? IntegrationEvent,
    bool DeserializationFailed,
    int HandlerCount,
    Exception? Exception)
{
    internal static EventBusDispatchResult Success(IntegrationEvent integrationEvent, int handlerCount) =>
        new(EventBusDispatchOutcome.Success, integrationEvent.GetType(), integrationEvent, false, handlerCount, null);

    internal static EventBusDispatchResult Retry(IntegrationEvent integrationEvent, int handlerCount, Exception exception) =>
        new(EventBusDispatchOutcome.Retry, integrationEvent.GetType(), integrationEvent, false, handlerCount, exception);

    internal static EventBusDispatchResult Retry(Type? integrationEventType, int handlerCount) =>
        new(EventBusDispatchOutcome.Retry, integrationEventType, null, false, handlerCount, null);

    internal static EventBusDispatchResult DeserializationFailure(
        Type integrationEventType,
        int handlerCount,
        bool skipDeserializationFailures) =>
        new(
            skipDeserializationFailures ? EventBusDispatchOutcome.Success : EventBusDispatchOutcome.Retry,
            integrationEventType,
            null,
            true,
            handlerCount,
            null);
}
