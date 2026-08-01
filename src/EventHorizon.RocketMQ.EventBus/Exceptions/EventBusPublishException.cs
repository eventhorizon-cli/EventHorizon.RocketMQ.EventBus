namespace EventHorizon.RocketMQ.EventBus.Exceptions;

/// <summary>
/// Represents a serialization or transport failure while publishing an integration event.
/// </summary>
public sealed class EventBusPublishException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="EventBusPublishException"/> class.
    /// </summary>
    /// <param name="integrationEventType">The concrete integration event type that failed to publish.</param>
    /// <param name="topic">The RocketMQ topic for the event.</param>
    /// <param name="tag">The RocketMQ tag for the event, or <see langword="null"/> when the event is untagged.</param>
    /// <param name="registrationName">The EventBus registration name, or <see langword="null"/> for the default registration.</param>
    /// <param name="transportResult">A non-exception transport result that caused the failure, if any.</param>
    /// <param name="innerException">The serialization or transport exception that caused the failure, if any.</param>
    public EventBusPublishException(
        Type integrationEventType,
        string topic,
        string? tag,
        string? registrationName,
        string? transportResult = null,
        Exception? innerException = null)
        : base(CreateMessage(integrationEventType, topic, tag, registrationName, transportResult), innerException)
    {
        ArgumentNullException.ThrowIfNull(integrationEventType);
        ArgumentNullException.ThrowIfNull(topic);

        IntegrationEventType = integrationEventType;
        Topic = topic;
        Tag = tag;
        RegistrationName = registrationName;
        TransportResult = transportResult;
    }

    /// <summary>
    /// Gets the concrete integration event type that failed to publish.
    /// </summary>
    public Type IntegrationEventType { get; }

    /// <summary>
    /// Gets the RocketMQ topic for the failed event.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the RocketMQ tag for the failed event, or <see langword="null"/> when the event is untagged.
    /// </summary>
    public string? Tag { get; }

    /// <summary>
    /// Gets the EventBus registration name, or <see langword="null"/> for the default registration.
    /// </summary>
    public string? RegistrationName { get; }

    /// <summary>
    /// Gets the non-exception transport result that caused the failure, if any.
    /// </summary>
    public string? TransportResult { get; }

    private static string CreateMessage(
        Type integrationEventType,
        string topic,
        string? tag,
        string? registrationName,
        string? transportResult)
    {
        ArgumentNullException.ThrowIfNull(integrationEventType);
        ArgumentNullException.ThrowIfNull(topic);

        var registration = registrationName ?? "<default>";
        var tagDisplay = tag ?? "<none>";
        var message = $"Publishing integration event '{integrationEventType.FullName}' to topic '{topic}' with tag '{tagDisplay}' failed for registration '{registration}'.";
        return transportResult is null ? message : $"{message} Transport result: '{transportResult}'.";
    }
}
