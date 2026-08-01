namespace EventHorizon.RocketMQ.EventBus.Internal.Dispatching;

/// <summary>
/// Dispatches one received transport message through an EventBus registration's existing asynchronous scope.
/// </summary>
internal interface IEventBusDispatchRuntime
{
    /// <summary>
    /// Dispatches a received message after resolving its application route from topic and tag metadata.
    /// </summary>
    /// <param name="topic">The received RocketMQ topic.</param>
    /// <param name="tag">The received RocketMQ tag.</param>
    /// <param name="payload">The received message body.</param>
    /// <param name="cancellationToken">The current transport delivery token.</param>
    /// <returns>The transport-neutral dispatch result.</returns>
    ValueTask<EventBusDispatchResult> DispatchAsync(
        string? topic,
        string? tag,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Dispatches a received message after the adapter has selected an expected integration event type.
    /// </summary>
    /// <param name="integrationEventType">The exact event type selected from the immutable route plan.</param>
    /// <param name="topic">The received RocketMQ topic.</param>
    /// <param name="tag">The received RocketMQ tag.</param>
    /// <param name="payload">The received message body.</param>
    /// <param name="cancellationToken">The current transport delivery token.</param>
    /// <returns>The transport-neutral dispatch result.</returns>
    ValueTask<EventBusDispatchResult> DispatchAsync(
        Type integrationEventType,
        string? topic,
        string? tag,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default);
}
