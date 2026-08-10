namespace EventHorizon.RocketMQ.Grpc.EventBus;

/// <summary>
/// Configures the gRPC Producer owned by one EventBus registration.
/// </summary>
public sealed class GrpcEventBusProducerOptions
{
    /// <summary>
    /// Gets or sets the timeout for sending messages.
    /// </summary>
    public TimeSpan SendMsgTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Gets or sets the number of retries before a send operation fails.
    /// </summary>
    /// <remarks>Retries may produce duplicate messages; consumers must process side effects idempotently.</remarks>
    public int RetryTimesWhenSendFailed { get; set; } = 2;

    /// <summary>
    /// Gets or sets the maximum allowed message size in bytes.
    /// </summary>
    public int MaxMessageSize { get; set; } = 4 * 1024 * 1024;

    internal GrpcEventBusProducerOptions Snapshot() => new()
    {
        SendMsgTimeout = SendMsgTimeout,
        RetryTimesWhenSendFailed = RetryTimesWhenSendFailed,
        MaxMessageSize = MaxMessageSize,
    };

    internal void ApplyTo(GrpcProducerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.SendMsgTimeout = SendMsgTimeout;
        options.RetryTimesWhenSendFailed = RetryTimesWhenSendFailed;
        options.MaxMessageSize = MaxMessageSize;
    }
}
