using System.Text;
using Xunit;

namespace EventHorizon.RocketMQ.EventBus.Tests.Logging;

public sealed class EventBusPayloadJsonFormatterTests
{
    [Fact]
    public void Format_CompactsAJsonPayload()
    {
        var payload = Encoding.UTF8.GetBytes("""
            {
              "Value": "published",
              "Count": 2
            }
            """);

        var result = EventBusPayloadJsonFormatter.Format(payload);

        Assert.Equal("{\"Value\":\"published\",\"Count\":2}", result);
    }

    [Fact]
    public void Format_WrapsANonJsonPayloadAsBase64Json()
    {
        var payload = new byte[] { 0xc3, 0x28, 0x00 };

        var result = EventBusPayloadJsonFormatter.Format(payload);

        Assert.Equal("{\"encoding\":\"base64\",\"data\":\"wygA\"}", result);
    }

    [Fact]
    public void Format_UsesTheDefaultJsonViewForACustomSerializer()
    {
        var integrationEvent = new LoggingEvent { Value = "custom" };

        var result = EventBusPayloadJsonFormatter.Format(
            new BinarySerializer(),
            integrationEvent,
            new byte[] { 0xff, 0x01 });

        Assert.Equal("{\"Value\":\"custom\"}", result);
    }

    [Fact]
    public void Format_FallsBackToTheWirePayloadWhenTheDefaultJsonViewFails()
    {
        var integrationEvent = new LoggingEvent();
        integrationEvent.Value = integrationEvent;

        var result = EventBusPayloadJsonFormatter.Format(
            new BinarySerializer(),
            integrationEvent,
            new byte[] { 0xff, 0x01 });

        Assert.Equal("{\"encoding\":\"base64\",\"data\":\"/wE=\"}", result);
    }

    private sealed class LoggingEvent : IntegrationEvent
    {
        public LoggingEvent()
            : base("logging")
        {
        }

        public object? Value { get; set; }
    }

    private sealed class BinarySerializer : IIntegrationEventSerializer
    {
        public byte[] Serialize(IntegrationEvent integrationEvent) => [0xff, 0x01];

        public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType) =>
            throw new NotSupportedException();
    }
}
