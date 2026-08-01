namespace EventHorizon.RocketMQ.EventBus.Abstractions;

/// <summary>
/// Handles one type of integration event.
/// </summary>
/// <typeparam name="TIntegrationEvent">The integration event type handled by the implementation.</typeparam>
public interface IIntegrationEventBusHandler<in TIntegrationEvent>
    where TIntegrationEvent : IntegrationEvent
{
    /// <summary>
    /// Handles an integration event.
    /// </summary>
    /// <param name="integrationEvent">The deserialized integration event.</param>
    /// <param name="cancellationToken">The token for the current delivery attempt.</param>
    /// <returns>A task that completes when handling is complete.</returns>
    Task HandleAsync(TIntegrationEvent integrationEvent, CancellationToken cancellationToken = default);
}
