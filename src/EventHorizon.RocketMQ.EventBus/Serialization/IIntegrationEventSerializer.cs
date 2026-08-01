namespace EventHorizon.RocketMQ.EventBus.Serialization;

/// <summary>
/// Serializes and deserializes integration event message bodies.
/// </summary>
/// <remarks>Implementations registered through <see cref="EventBusBuilderExtensions.UseSerializer{TSerializer}(IEventBusBuilder)"/> must be thread-safe.</remarks>
public interface IIntegrationEventSerializer
{
    /// <summary>
    /// Serializes an integration event to its transport message body.
    /// </summary>
    /// <param name="integrationEvent">The event to serialize.</param>
    /// <returns>The serialized message body.</returns>
    byte[] Serialize(IntegrationEvent integrationEvent);

    /// <summary>
    /// Deserializes a transport message body to the event type selected by the route table.
    /// </summary>
    /// <param name="payload">The message body.</param>
    /// <param name="integrationEventType">The exact registered integration event type.</param>
    /// <returns>The deserialized integration event.</returns>
    IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType);
}
