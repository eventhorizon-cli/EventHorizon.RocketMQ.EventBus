using System.Diagnostics;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Consumer;

internal sealed class RemotingEventBusPushMessageHandler<TAnchorHandler>(
    EventBusRegistrationAccessor<TAnchorHandler> registrationAccessor,
    ILogger<RemotingEventBusPushMessageHandler<TAnchorHandler>> logger) : IRemotingPushMessageHandler
    where TAnchorHandler : class
{
    private readonly EventBusRegistrationAccessor<TAnchorHandler> _registrationAccessor =
        registrationAccessor ?? throw new ArgumentNullException(nameof(registrationAccessor));
    private readonly ILogger<RemotingEventBusPushMessageHandler<TAnchorHandler>> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    public async ValueTask<ConsumeResult> HandleAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(context);

        var loggingSettings = _registrationAccessor.LoggingSettings;
        if (messages.Count != 1)
        {
            _logger.LogEventBusInvalidBatch(loggingSettings, messages.Count);
            return ConsumeResult.Retry;
        }

        var message = messages[0];
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            var dispatchResult = await _registrationAccessor.Dispatcher.DispatchAsync(
                message.Topic,
                message.Tag,
                message.Body,
                cancellationToken).ConfigureAwait(false);
            var consumeResult = RemotingEventBusDispatchOutcomeMapper.Map(dispatchResult.Outcome);

            _logger.LogEventBusConsumeCompleted(
                loggingSettings,
                _registrationAccessor.Serializer,
                dispatchResult,
                consumeResult,
                message,
                Stopwatch.GetElapsedTime(startedAt));
            return consumeResult;
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
                message,
                Stopwatch.GetElapsedTime(startedAt));
            return ConsumeResult.Retry;
        }
    }
}
