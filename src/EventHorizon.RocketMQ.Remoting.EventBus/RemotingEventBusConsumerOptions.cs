namespace EventHorizon.RocketMQ.Remoting.EventBus;

/// <summary>
/// Configures the classic Remoting Push consumer owned by one EventBus registration.
/// </summary>
public sealed class RemotingEventBusConsumerOptions
{
    /// <summary>
    /// Gets or sets the consumer group name.
    /// </summary>
    public string GroupName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets where consumption starts when a queue has no committed offset.
    /// </summary>
    public ConsumeFromPosition InitialPosition { get; set; } = ConsumeFromPosition.End;

    /// <summary>
    /// Gets or sets the timestamp used when <see cref="InitialPosition"/> is
    /// <see cref="ConsumeFromPosition.Timestamp"/>.
    /// </summary>
    public DateTimeOffset? ConsumeTimestamp { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of message-handler dispatches that may run concurrently.
    /// </summary>
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum number of messages requested by each PULL operation.
    /// </summary>
    public int PullBatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum number of messages requested by each Broker-assigned POP operation.
    /// </summary>
    public int PopBatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the initial invisibility duration for a message received through POP.
    /// </summary>
    public TimeSpan PopInvisibleDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Gets or sets the maximum number of unsettled POP messages for one Broker assignment.
    /// </summary>
    public int PopMaxInflightMessagesPerAssignment { get; set; } = 96;

    /// <summary>
    /// Gets or sets the maximum number of newly pulled messages waiting for their first dispatch.
    /// </summary>
    public int PullMaxCachedMessages { get; set; } = 1024;

    /// <summary>
    /// Gets or sets the maximum total body size, in bytes, of pulled messages waiting for first dispatch.
    /// </summary>
    public int PullMaxCachedMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum message payload size, in bytes, requested in a PULL response.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum delivery attempt at which protocol-specific terminal retry handling begins.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 16;

    /// <summary>
    /// Gets or sets how long the Broker may hold a receive request while waiting for messages.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the local delay used by recoverable receive and settlement work.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets the maximum time a concurrent message dispatch may run before retry is requested.
    /// </summary>
    public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Gets or sets whether queue assignments are calculated by the client or queried from the Broker.
    /// </summary>
    public RemotingPushQueueAssignmentMode QueueAssignmentMode { get; set; } = RemotingPushQueueAssignmentMode.Client;

    /// <summary>
    /// Gets or sets whether payloads that cannot be deserialized are logged and acknowledged without invoking an
    /// application handler.
    /// </summary>
    /// <remarks>
    /// The default is <see langword="true"/>. Set this property to <see langword="false"/> to request normal transport
    /// redelivery. EventBus does not request direct dead-letter settlement in either case.
    /// </remarks>
    public bool SkipDeserializationFailures { get; set; } = true;

    internal RemotingEventBusConsumerOptions Snapshot() => new()
    {
        GroupName = GroupName,
        InitialPosition = InitialPosition,
        ConsumeTimestamp = ConsumeTimestamp,
        MaxConcurrency = MaxConcurrency,
        PullBatchSize = PullBatchSize,
        PopBatchSize = PopBatchSize,
        PopInvisibleDuration = PopInvisibleDuration,
        PopMaxInflightMessagesPerAssignment = PopMaxInflightMessagesPerAssignment,
        PullMaxCachedMessages = PullMaxCachedMessages,
        PullMaxCachedMessageBytes = PullMaxCachedMessageBytes,
        MaxMessageBytes = MaxMessageBytes,
        MaxDeliveryAttempts = MaxDeliveryAttempts,
        LongPollingTimeout = LongPollingTimeout,
        RetryDelay = RetryDelay,
        ConsumeTimeout = ConsumeTimeout,
        QueueAssignmentMode = QueueAssignmentMode,
        SkipDeserializationFailures = SkipDeserializationFailures,
    };

    internal void ApplyTo(RemotingPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.GroupName = GroupName;
        options.InitialPosition = InitialPosition;
        options.ConsumeTimestamp = ConsumeTimestamp;
        options.MaxConcurrency = MaxConcurrency;
        options.PullBatchSize = PullBatchSize;
        options.PopBatchSize = PopBatchSize;
        options.PopInvisibleDuration = PopInvisibleDuration;
        options.PopMaxInflightMessagesPerAssignment = PopMaxInflightMessagesPerAssignment;
        options.PullMaxCachedMessages = PullMaxCachedMessages;
        options.PullMaxCachedMessageBytes = PullMaxCachedMessageBytes;
        options.MaxMessageBytes = MaxMessageBytes;
        options.MaxDeliveryAttempts = MaxDeliveryAttempts;
        options.LongPollingTimeout = LongPollingTimeout;
        options.RetryDelay = RetryDelay;
        options.ConsumeTimeout = ConsumeTimeout;
        options.QueueAssignmentMode = QueueAssignmentMode;
        options.ConsumerMode = ConsumerMode.Clustering;
        options.ConsumeOrderly = false;
        options.ConsumeMessageBatchSize = 1;
    }
}
