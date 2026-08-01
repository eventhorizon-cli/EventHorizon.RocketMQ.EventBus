using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Xunit;

namespace EventHorizon.RocketMQ.EventBus.Tests.Serialization;

public sealed class NewtonsoftJsonIntegrationEventSerializerTests
{
    private readonly NewtonsoftJsonIntegrationEventSerializer _serializer = new();

    [Fact]
    public void Serialize_UsesTheFixedCompactUtf8WireContract()
    {
        var integrationEvent = new WireContractEvent
        {
            OrderId = Guid.Parse("353c0bcb-7f6d-49a2-8dd7-d144bee5366a"),
            Total = 128.50m,
            Note = null,
            Quantity = 0,
        };

        var payload = _serializer.Serialize(integrationEvent);

        Assert.Equal(
            "{\"OrderId\":\"353c0bcb-7f6d-49a2-8dd7-d144bee5366a\",\"Total\":128.50,\"Note\":null,\"Quantity\":0}",
            Encoding.UTF8.GetString(payload));
        Assert.DoesNotContain("Topic", Encoding.UTF8.GetString(payload), StringComparison.Ordinal);
        Assert.DoesNotContain("Tag", Encoding.UTF8.GetString(payload), StringComparison.Ordinal);
        Assert.DoesNotContain("$type", Encoding.UTF8.GetString(payload), StringComparison.Ordinal);
    }

    [Fact]
    public void Deserialize_UsesTheRouteSelectedTypeAndSupportsAdditiveSchemaEvolution()
    {
        var payload = Encoding.UTF8.GetBytes(
            "{\"OrderId\":\"353c0bcb-7f6d-49a2-8dd7-d144bee5366a\",\"Extra\":true}");

        var integrationEvent = Assert.IsType<WireContractEvent>(_serializer.Deserialize(payload, typeof(WireContractEvent)));

        Assert.Equal(Guid.Parse("353c0bcb-7f6d-49a2-8dd7-d144bee5366a"), integrationEvent.OrderId);
        Assert.Equal(0m, integrationEvent.Total);
        Assert.Null(integrationEvent.Note);
        Assert.Equal(0, integrationEvent.Quantity);
        Assert.Equal("wire", integrationEvent.Topic);
        Assert.Equal("contract", integrationEvent.Tag);
    }

    [Fact]
    public void RoundTrip_PreservesAnUntaggedEventThroughItsConstructorInsteadOfThePayload()
    {
        var payload = _serializer.Serialize(new UntaggedOrderEvent());

        var integrationEvent = Assert.IsType<UntaggedOrderEvent>(
            _serializer.Deserialize(payload, typeof(UntaggedOrderEvent)));

        Assert.Equal("{}", Encoding.UTF8.GetString(payload));
        Assert.Equal("orders", integrationEvent.Topic);
        Assert.Null(integrationEvent.Tag);
    }

    [Fact]
    public void Deserialize_IgnoresPayloadTypeMetadata()
    {
        var payload = Encoding.UTF8.GetBytes(
            "{\"$type\":\"System.Version, System.Private.CoreLib\",\"OrderId\":\"353c0bcb-7f6d-49a2-8dd7-d144bee5366a\"}");

        var integrationEvent = Assert.IsType<WireContractEvent>(_serializer.Deserialize(payload, typeof(WireContractEvent)));

        Assert.Equal(Guid.Parse("353c0bcb-7f6d-49a2-8dd7-d144bee5366a"), integrationEvent.OrderId);
    }

    [Fact]
    public void Deserialize_RejectsMalformedUtf8()
    {
        var payload = new byte[] { 0xc3, 0x28 };

        Assert.Throws<DecoderFallbackException>(() => _serializer.Deserialize(payload, typeof(WireContractEvent)));
    }

    [Fact]
    public void Deserialize_RejectsNonIntegrationEventDestination()
    {
        var payload = Encoding.UTF8.GetBytes("{}");

        Assert.Throws<ArgumentException>(() => _serializer.Deserialize(payload, typeof(string)));
    }

    [Fact]
    public void Serialize_DoesNotReadJsonConvertProcessDefaults()
    {
        var originalSettings = JsonConvert.DefaultSettings;
        JsonConvert.DefaultSettings = static () => new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
        };

        try
        {
            var payload = _serializer.Serialize(new WireContractEvent
            {
                OrderId = Guid.Parse("353c0bcb-7f6d-49a2-8dd7-d144bee5366a"),
            });

            Assert.Equal(
                "{\"OrderId\":\"353c0bcb-7f6d-49a2-8dd7-d144bee5366a\",\"Total\":0.0,\"Note\":null,\"Quantity\":0}",
                Encoding.UTF8.GetString(payload));
        }
        finally
        {
            JsonConvert.DefaultSettings = originalSettings;
        }
    }

    [Fact]
    public void Deserialize_EnforcesTheConfiguredMaximumReadDepth()
    {
        var nestedValue = new string('[', 65) + "0" + new string(']', 65);
        var payload = Encoding.UTF8.GetBytes($"{{\"Payload\":{nestedValue}}}");

        Assert.ThrowsAny<JsonException>(() => _serializer.Deserialize(payload, typeof(DepthContractEvent)));
    }
}
