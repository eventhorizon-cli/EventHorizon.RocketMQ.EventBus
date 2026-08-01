using System.Globalization;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace EventHorizon.RocketMQ.EventBus.Serialization;

/// <summary>
/// Implements the default compact JSON and UTF-8 integration-event wire contract.
/// </summary>
public sealed class NewtonsoftJsonIntegrationEventSerializer : IIntegrationEventSerializer
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private static readonly JsonSerializerSettings Settings = CreateSettings();

    /// <inheritdoc />
    public byte[] Serialize(IntegrationEvent integrationEvent)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);

        var json = JsonConvert.SerializeObject(integrationEvent, integrationEvent.GetType(), Settings);
        return StrictUtf8.GetBytes(json);
    }

    /// <inheritdoc />
    public IntegrationEvent Deserialize(ReadOnlyMemory<byte> payload, Type integrationEventType)
    {
        ArgumentNullException.ThrowIfNull(integrationEventType);

        if (!typeof(IntegrationEvent).IsAssignableFrom(integrationEventType) || integrationEventType.IsAbstract)
        {
            throw new ArgumentException("The destination type must be a concrete integration event type.", nameof(integrationEventType));
        }

        var json = StrictUtf8.GetString(payload.Span);
        var deserialized = JsonConvert.DeserializeObject(json, integrationEventType, Settings);
        if (deserialized is not IntegrationEvent integrationEvent || integrationEvent.GetType() != integrationEventType)
        {
            throw new JsonSerializationException("The payload did not produce the exact integration event type selected by the route table.");
        }

        return integrationEvent;
    }

    private static JsonSerializerSettings CreateSettings() => new()
    {
        TypeNameHandling = TypeNameHandling.None,
        MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
        ContractResolver = new DefaultContractResolver(),
        Formatting = Formatting.None,
        NullValueHandling = NullValueHandling.Include,
        DefaultValueHandling = DefaultValueHandling.Include,
        MissingMemberHandling = MissingMemberHandling.Ignore,
        DateFormatHandling = DateFormatHandling.IsoDateFormat,
        DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind,
        Culture = CultureInfo.InvariantCulture,
        ReferenceLoopHandling = ReferenceLoopHandling.Error,
        PreserveReferencesHandling = PreserveReferencesHandling.None,
        MaxDepth = 64,
    };
}
