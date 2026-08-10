namespace EventHorizon.RocketMQ.EventBus.Internal.Dispatching;

/// <summary>
/// Describes the transport-neutral result of processing one delivery.
/// </summary>
internal enum EventBusDispatchOutcome
{
    /// <summary>
    /// The delivery should be acknowledged because every application handler completed or a configured
    /// deserialization failure was skipped.
    /// </summary>
    Success,

    /// <summary>
    /// The delivery failed and the transport should use its normal retry path.
    /// </summary>
    Retry,
}
