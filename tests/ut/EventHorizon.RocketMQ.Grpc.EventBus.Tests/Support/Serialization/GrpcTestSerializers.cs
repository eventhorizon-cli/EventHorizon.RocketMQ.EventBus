namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Support.Serialization;

internal sealed class ThrowingGrpcSerializer : IIntegrationEventSerializer
{
    public byte[] Serialize(IntegrationEvent integrationEvent) =>
        throw new InvalidOperationException("Expected serializer failure.");

    public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType) =>
        throw new NotSupportedException();
}

internal sealed class GrpcBinarySerializer : IIntegrationEventSerializer
{
    internal static byte[] WirePayload => [0xff, 0x01, 0x02];

    public byte[] Serialize(IntegrationEvent integrationEvent) => WirePayload;

    public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType)
    {
        Assert.Equal(WirePayload, payload.ToArray());
        Assert.Equal(typeof(GrpcDispatchEvent), integrationEventType);
        return new GrpcDispatchEvent { Value = "custom-consumed" };
    }
}
