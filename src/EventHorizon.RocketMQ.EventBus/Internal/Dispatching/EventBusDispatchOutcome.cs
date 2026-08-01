namespace EventHorizon.RocketMQ.EventBus.Internal.Dispatching;

/// <summary>
/// Describes the transport-neutral result of processing one delivery.
/// </summary>
internal enum EventBusDispatchOutcome
{
    /// <summary>
    /// Every application handler completed successfully.
    /// </summary>
    Success,

    /// <summary>
    /// A handler or one of its dependencies failed and the transport should retry delivery.
    /// </summary>
    Retry,

    /// <summary>
    /// The route or payload is invalid and the transport should dead-letter delivery.
    /// </summary>
    DeadLetter,
}
