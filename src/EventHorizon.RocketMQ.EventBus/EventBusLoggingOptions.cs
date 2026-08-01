namespace EventHorizon.RocketMQ.EventBus;

/// <summary>
/// Configures EventBus logs for one default or named registration.
/// </summary>
public sealed class EventBusLoggingOptions
{
    /// <summary>
    /// Gets or sets whether EventBus publish, Consumer, outcome, and subscription-summary logs are enabled.
    /// </summary>
    /// <remarks>Underlying RocketMQ client logs are not affected. The default is <see langword="true"/>.</remarks>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets whether publish and Consumer outcome logs include the complete message payload.
    /// </summary>
    /// <remarks>
    /// Payloads can contain sensitive application data. Deserialization-failure logs omit the payload regardless of
    /// this setting. The default is <see langword="true"/>.
    /// </remarks>
    public bool IncludePayload { get; set; } = true;
}
