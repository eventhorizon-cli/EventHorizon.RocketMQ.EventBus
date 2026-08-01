using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EventHorizon.RocketMQ.EventBus.Internal.Logging;

internal static class EventBusPayloadJsonFormatter
{
    private static readonly NewtonsoftJsonIntegrationEventSerializer DefaultSerializer = new();
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    internal static string Format(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var json = StrictUtf8.GetString(payload.Span);
            return JToken.Parse(json).ToString(Formatting.None);
        }
        catch (Exception exception) when (exception is DecoderFallbackException or JsonException)
        {
            return new JObject
            {
                ["encoding"] = "base64",
                ["data"] = Convert.ToBase64String(payload.Span),
            }.ToString(Formatting.None);
        }
    }

    internal static string? Format(
        IIntegrationEventSerializer? serializer,
        IntegrationEvent integrationEvent,
        ReadOnlyMemory<byte>? wirePayload)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);

        if (serializer is not NewtonsoftJsonIntegrationEventSerializer)
        {
            try
            {
                return Format(DefaultSerializer.Serialize(integrationEvent));
            }
            catch (Exception)
            {
                // Logging must not change publish or consume behavior; preserve the actual wire payload as a fallback.
            }
        }

        return wirePayload.HasValue ? Format(wirePayload.Value) : null;
    }
}
