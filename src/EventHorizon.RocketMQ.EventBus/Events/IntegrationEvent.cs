using Newtonsoft.Json;

namespace EventHorizon.RocketMQ.EventBus.Events;

/// <summary>
/// Represents a strongly typed integration event with immutable RocketMQ routing metadata.
/// </summary>
public abstract class IntegrationEvent
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IntegrationEvent"/> class.
    /// </summary>
    /// <param name="topic">The RocketMQ topic for the event.</param>
    /// <param name="tag">The literal RocketMQ tag for the event, or <see langword="null"/> for an untagged event.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">The supplied route metadata is blank or uses an unsupported tag expression.</exception>
    protected IntegrationEvent(string topic, string? tag = null)
    {
        ValidateRouteMetadata(topic, tag);
        Topic = topic;
        Tag = tag;
    }

    /// <summary>
    /// Gets the RocketMQ topic for the event.
    /// </summary>
    /// <remarks>This value is transport metadata and is not written to the default JSON message body.</remarks>
    [JsonIgnore]
    public string Topic { get; }

    /// <summary>
    /// Gets the literal RocketMQ tag for the event, or <see langword="null"/> when the event is untagged.
    /// </summary>
    /// <remarks>This value is transport metadata and is not written to the default JSON message body.</remarks>
    [JsonIgnore]
    public string? Tag { get; }

    internal static void ValidateRouteMetadata(string topic, string? tag)
    {
        ArgumentNullException.ThrowIfNull(topic);

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("The integration event topic cannot be blank.", nameof(topic));
        }

        if (tag is not null && string.IsNullOrWhiteSpace(tag))
        {
            throw new ArgumentException("The integration event tag cannot be blank.", nameof(tag));
        }

        if (topic == "*" || tag == "*")
        {
            throw new ArgumentException("Wildcard route metadata is not supported.", tag == "*" ? nameof(tag) : nameof(topic));
        }

        if (topic.Contains("||", StringComparison.Ordinal) || tag?.Contains("||", StringComparison.Ordinal) == true)
        {
            throw new ArgumentException("Route metadata must not contain a tag expression operator.");
        }
    }
}
