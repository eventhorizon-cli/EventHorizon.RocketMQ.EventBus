namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Support.Serialization;

internal sealed class ThrowingSerializer : IIntegrationEventSerializer
{
    public byte[] Serialize(IntegrationEvent integrationEvent) =>
        throw new InvalidOperationException("Expected serializer failure.");

    public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType) =>
        throw new NotSupportedException();
}

internal sealed class RemotingBinarySerializer : IIntegrationEventSerializer
{
    internal static byte[] WirePayload => [0xff, 0x03, 0x04];

    public byte[] Serialize(IntegrationEvent integrationEvent) => WirePayload;

    public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType)
    {
        Assert.Equal(WirePayload, payload.ToArray());
        Assert.Equal(typeof(RemotingTestEvent), integrationEventType);
        return new RemotingTestEvent { Value = "custom-consumed" };
    }
}
