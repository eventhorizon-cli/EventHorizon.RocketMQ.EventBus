namespace EventHorizon.RocketMQ.Grpc.EventBus;

/// <summary>
/// Configures the gRPC Push consumer owned by one EventBus registration.
/// </summary>
public sealed class GrpcEventBusConsumerOptions
{
    /// <summary>
    /// Gets or sets the consumer group name.
    /// </summary>
    public string GroupName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the maximum number of message-handler dispatches that may run concurrently.
    /// </summary>
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum number of messages requested by each receive operation.
    /// </summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum number of received messages buffered locally before dispatch.
    /// </summary>
    public int MaxCachedMessages { get; set; } = 1024;

    /// <summary>
    /// Gets or sets the maximum total body size, in bytes, of messages buffered locally before dispatch.
    /// </summary>
    public int MaxCachedMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the fallback maximum number of delivery attempts before terminal transport handling begins.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 16;

    /// <summary>
    /// Gets or sets the initial duration for which a received message remains invisible to other consumers.
    /// </summary>
    public TimeSpan InvisibleDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum time a message handler may run before redelivery is requested.
    /// </summary>
    public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Gets or sets the maximum time the server may hold a receive request while waiting for messages.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the fallback delay used by recoverable receive and settlement work.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets whether payloads that cannot be deserialized are logged and acknowledged without invoking an
    /// application handler.
    /// </summary>
    /// <remarks>
    /// The default is <see langword="true"/>. Set this property to <see langword="false"/> to request normal transport
    /// redelivery. EventBus does not request direct dead-letter settlement in either case.
    /// </remarks>
    public bool SkipDeserializationFailures { get; set; } = true;

    internal GrpcEventBusConsumerOptions Snapshot() => new()
    {
        GroupName = GroupName,
        MaxConcurrency = MaxConcurrency,
        BatchSize = BatchSize,
        MaxCachedMessages = MaxCachedMessages,
        MaxCachedMessageBytes = MaxCachedMessageBytes,
        MaxDeliveryAttempts = MaxDeliveryAttempts,
        InvisibleDuration = InvisibleDuration,
        ConsumeTimeout = ConsumeTimeout,
        LongPollingTimeout = LongPollingTimeout,
        RetryDelay = RetryDelay,
        SkipDeserializationFailures = SkipDeserializationFailures,
    };

    internal void ApplyTo(GrpcPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.GroupName = GroupName;
        options.MaxConcurrency = MaxConcurrency;
        options.BatchSize = BatchSize;
        options.MaxCachedMessages = MaxCachedMessages;
        options.MaxCachedMessageBytes = MaxCachedMessageBytes;
        options.MaxDeliveryAttempts = MaxDeliveryAttempts;
        options.InvisibleDuration = InvisibleDuration;
        options.ConsumeTimeout = ConsumeTimeout;
        options.LongPollingTimeout = LongPollingTimeout;
        options.RetryDelay = RetryDelay;
    }
}
