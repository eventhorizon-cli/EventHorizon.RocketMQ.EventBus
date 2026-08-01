namespace EventHorizon.RocketMQ.EventBus.Tests.Support.Serialization;

internal sealed class AlternateSerializer : IIntegrationEventSerializer
{
    private readonly NewtonsoftJsonIntegrationEventSerializer _inner = new();

    public byte[] Serialize(IntegrationEvent integrationEvent) => _inner.Serialize(integrationEvent);

    public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType) =>
        _inner.Deserialize(payload, integrationEventType);
}
