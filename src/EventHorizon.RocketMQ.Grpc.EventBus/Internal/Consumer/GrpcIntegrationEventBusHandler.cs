using System.Diagnostics;

namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Consumer;

internal sealed class GrpcIntegrationEventBusHandler<TAnchorHandler>(
    EventBusRegistrationAccessor<TAnchorHandler> accessor,
    ILogger<GrpcIntegrationEventBusHandler<TAnchorHandler>> logger) : IGrpcPushMessageHandler
    where TAnchorHandler : class
{
    private readonly EventBusRegistrationAccessor<TAnchorHandler> _accessor =
        accessor ?? throw new ArgumentNullException(nameof(accessor));
    private readonly ILogger<GrpcIntegrationEventBusHandler<TAnchorHandler>> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        return DispatchAsync(
            message.Topic,
            message.Tag,
            message.Body,
            message.MessageId,
            message.DeliveryAttempt,
            cancellationToken,
            message.BrokerName,
            message.QueueId,
            message.QueueOffset);
    }

    internal async ValueTask<ConsumeResult> DispatchAsync(
        string? topic,
        string? tag,
        ReadOnlyMemory<byte> body,
        string? messageId,
        int deliveryAttempt,
        CancellationToken cancellationToken,
        string? brokerName = null,
        int? queueId = null,
        long? queueOffset = null)
    {
        var loggingSettings = _accessor.LoggingSettings;
        var startedAt = Stopwatch.GetTimestamp();

        try
        {
            var result = await _accessor.Dispatcher
                .DispatchAsync(topic, tag, body, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogEventBusConsumeCompleted(
                loggingSettings,
                _accessor.Serializer,
                result,
                topic,
                tag,
                messageId,
                brokerName,
                queueId,
                queueOffset,
                deliveryAttempt,
                Stopwatch.GetElapsedTime(startedAt),
                body);
            return GrpcEventBusConsumeResultMapper.Map(result.Outcome);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogEventBusUnhandledRetry(
                loggingSettings,
                exception,
                topic,
                tag,
                messageId,
                brokerName,
                queueId,
                queueOffset,
                deliveryAttempt,
                Stopwatch.GetElapsedTime(startedAt),
                body);

            return ConsumeResult.Retry;
        }
    }
}
