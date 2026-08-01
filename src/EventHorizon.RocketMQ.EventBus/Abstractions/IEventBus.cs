namespace EventHorizon.RocketMQ.EventBus.Abstractions;

/// <summary>
/// Publishes integration events through the configured RocketMQ transport adapter.
/// </summary>
public interface IEventBus
{
    /// <summary>
    /// Publishes an integration event.
    /// </summary>
    /// <param name="integrationEvent">The event to publish.</param>
    /// <param name="cancellationToken">The token that cancels waiting for the local publish operation.</param>
    /// <returns>A task that completes after the local transport publish operation completes.</returns>
    /// <exception cref="EventBusPublishException">Serialization or transport publishing failed.</exception>
    Task PublishAsync(IntegrationEvent integrationEvent, CancellationToken cancellationToken = default);
}
