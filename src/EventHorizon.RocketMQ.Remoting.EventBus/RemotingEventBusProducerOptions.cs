namespace EventHorizon.RocketMQ.Remoting.EventBus;

/// <summary>
/// Configures the classic Remoting Producer owned by one EventBus registration.
/// </summary>
public sealed class RemotingEventBusProducerOptions
{
    /// <summary>
    /// Gets or sets the Producer group name.
    /// </summary>
    public string GroupName { get; set; } = "DEFAULT_PRODUCER";

    /// <summary>
    /// Gets or sets the number of queues created for an automatically created Topic.
    /// </summary>
    public int DefaultTopicQueueNums { get; set; } = 4;

    /// <summary>
    /// Gets or sets the timeout for sending messages.
    /// </summary>
    public TimeSpan SendMsgTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Gets or sets the message body size at which compression is enabled.
    /// </summary>
    public int CompressMsgBodyOverHowmuch { get; set; } = 4 * 1024;

    /// <summary>
    /// Gets or sets the number of retries before a send operation fails.
    /// </summary>
    /// <remarks>Retries may produce duplicate messages; consumers must process side effects idempotently.</remarks>
    public int RetryTimesWhenSendFailed { get; set; } = 2;

    /// <summary>
    /// Gets or sets the maximum allowed message size in bytes.
    /// </summary>
    public int MaxMessageSize { get; set; } = 4 * 1024 * 1024;

    internal RemotingEventBusProducerOptions Snapshot() => new()
    {
        GroupName = GroupName,
        DefaultTopicQueueNums = DefaultTopicQueueNums,
        SendMsgTimeout = SendMsgTimeout,
        CompressMsgBodyOverHowmuch = CompressMsgBodyOverHowmuch,
        RetryTimesWhenSendFailed = RetryTimesWhenSendFailed,
        MaxMessageSize = MaxMessageSize,
    };

    internal void ApplyTo(RemotingProducerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.GroupName = GroupName;
        options.DefaultTopicQueueNums = DefaultTopicQueueNums;
        options.SendMsgTimeout = SendMsgTimeout;
        options.CompressMsgBodyOverHowmuch = CompressMsgBodyOverHowmuch;
        options.RetryTimesWhenSendFailed = RetryTimesWhenSendFailed;
        options.MaxMessageSize = MaxMessageSize;
    }
}
