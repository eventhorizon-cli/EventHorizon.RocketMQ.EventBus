namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Logging;

internal static class RemotingEventBusLoggerExtensions
{
    internal static void LogEventBusPublishSucceeded(
        this ILogger logger,
        EventBusLoggingSettings settings,
        IntegrationEvent integrationEvent,
        string? messageId,
        TimeSpan duration,
        IIntegrationEventSerializer serializer,
        ReadOnlyMemory<byte> payload) =>
        Write(logger, settings, LogLevel.Information, () =>
        {
            if (settings.IncludePayload)
            {
                RemotingEventBusLogMessages.PublishSucceededWithPayload(
                    logger,
                    integrationEvent.Topic,
                    integrationEvent.Tag,
                    messageId,
                    duration,
                    EventBusPayloadJsonFormatter.Format(serializer, integrationEvent, payload));
                return;
            }

            RemotingEventBusLogMessages.PublishSucceeded(
                logger,
                integrationEvent.Topic,
                integrationEvent.Tag,
                messageId,
                duration);
        });

    internal static void LogEventBusPublishFailed(
        this ILogger logger,
        EventBusLoggingSettings settings,
        EventBusPublishException exception,
        IntegrationEvent integrationEvent,
        string? messageId,
        TimeSpan duration,
        IIntegrationEventSerializer? serializer,
        ReadOnlyMemory<byte>? payload) =>
        Write(logger, settings, LogLevel.Error, () =>
        {
            if (settings.IncludePayload)
            {
                RemotingEventBusLogMessages.PublishFailedWithPayload(
                    logger,
                    exception,
                    exception.Topic,
                    exception.Tag,
                    messageId,
                    exception.TransportResult,
                    duration,
                    EventBusPayloadJsonFormatter.Format(serializer, integrationEvent, payload));
                return;
            }

            RemotingEventBusLogMessages.PublishFailed(
                logger,
                exception,
                exception.Topic,
                exception.Tag,
                messageId,
                exception.TransportResult,
                duration);
        });

    internal static void LogEventBusInvalidBatch(
        this ILogger logger,
        EventBusLoggingSettings settings,
        int messageCount) =>
        Write(logger, settings, LogLevel.Error, () =>
            RemotingEventBusLogMessages.ConsumeInvalidBatch(logger, messageCount));

    internal static void LogEventBusUnhandledRetry(
        this ILogger logger,
        EventBusLoggingSettings settings,
        Exception exception,
        RemotingMessageView message,
        TimeSpan duration) =>
        Write(logger, settings, LogLevel.Error, () =>
        {
            if (settings.IncludePayload)
            {
                RemotingEventBusLogMessages.ConsumeUnhandledRetryWithPayload(
                    logger,
                    exception,
                    message.Topic,
                    message.Tag,
                    message.MessageId,
                    message.BrokerName,
                    message.QueueId,
                    message.QueueOffset,
                    message.DeliveryAttempt,
                    ConsumeResult.Retry.ToString(),
                    duration,
                    EventBusPayloadJsonFormatter.Format(message.Body));
                return;
            }

            RemotingEventBusLogMessages.ConsumeUnhandledRetry(
                logger,
                exception,
                message.Topic,
                message.Tag,
                message.MessageId,
                message.BrokerName,
                message.QueueId,
                message.QueueOffset,
                message.DeliveryAttempt,
                ConsumeResult.Retry.ToString(),
                duration);
        });

    internal static void LogEventBusConsumeCompleted(
        this ILogger logger,
        EventBusLoggingSettings settings,
        IIntegrationEventSerializer serializer,
        EventBusDispatchResult result,
        ConsumeResult outcome,
        RemotingMessageView message,
        TimeSpan duration)
    {
        var level = outcome == ConsumeResult.Success ? LogLevel.Information : LogLevel.Error;
        Write(logger, settings, level, () => WriteConsumeCompleted(
            logger,
            settings,
            serializer,
            result,
            outcome,
            message,
            duration));
    }

    internal static void LogEventBusSubscriptionsMaterialized(
        this ILogger logger,
        EventBusLoggingSettings settings,
        string registrationName,
        string consumerGroup,
        int handlerCount,
        IReadOnlyList<string> subscriptions) =>
        Write(logger, settings, LogLevel.Information, () => RemotingEventBusLogMessages.SubscriptionsMaterialized(
            logger,
            registrationName,
            consumerGroup,
            handlerCount,
            subscriptions.Count,
            string.Join("; ", subscriptions)));

    private static void WriteConsumeCompleted(
        ILogger logger,
        EventBusLoggingSettings settings,
        IIntegrationEventSerializer serializer,
        EventBusDispatchResult result,
        ConsumeResult outcome,
        RemotingMessageView message,
        TimeSpan duration)
    {
        if (result.DeserializationFailed)
        {
            RemotingEventBusLogMessages.PayloadDeserializationFailed(
                logger,
                message.Topic,
                message.Tag,
                message.MessageId,
                message.BrokerName,
                message.QueueId,
                message.QueueOffset,
                message.DeliveryAttempt,
                outcome.ToString(),
                duration);
            return;
        }

        switch (outcome)
        {
            case ConsumeResult.Success:
                WriteSucceeded();
                return;
            case ConsumeResult.Retry:
                WriteRetry();
                return;
            case ConsumeResult.DeadLetter:
                WriteDeadLetter();
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unknown Remoting consume result.");
        }

        void WriteSucceeded()
        {
            if (settings.IncludePayload)
            {
                RemotingEventBusLogMessages.ConsumeSucceededWithPayload(
                    logger,
                    message.Topic,
                    message.Tag,
                    message.MessageId,
                    message.BrokerName,
                    message.QueueId,
                    message.QueueOffset,
                    message.DeliveryAttempt,
                    outcome.ToString(),
                    duration,
                    FormatPayload(serializer, result, message.Body));
                return;
            }

            RemotingEventBusLogMessages.ConsumeSucceeded(
                logger,
                message.Topic,
                message.Tag,
                message.MessageId,
                message.BrokerName,
                message.QueueId,
                message.QueueOffset,
                message.DeliveryAttempt,
                outcome.ToString(),
                duration);
        }

        void WriteRetry()
        {
            if (settings.IncludePayload)
            {
                RemotingEventBusLogMessages.ConsumeRetryWithPayload(
                    logger,
                    result.Exception,
                    message.Topic,
                    message.Tag,
                    message.MessageId,
                    message.BrokerName,
                    message.QueueId,
                    message.QueueOffset,
                    message.DeliveryAttempt,
                    outcome.ToString(),
                    duration,
                    FormatPayload(serializer, result, message.Body));
                return;
            }

            RemotingEventBusLogMessages.ConsumeRetry(
                logger,
                result.Exception,
                message.Topic,
                message.Tag,
                message.MessageId,
                message.BrokerName,
                message.QueueId,
                message.QueueOffset,
                message.DeliveryAttempt,
                outcome.ToString(),
                duration);
        }

        void WriteDeadLetter()
        {
            if (settings.IncludePayload)
            {
                RemotingEventBusLogMessages.ConsumeDeadLetterWithPayload(
                    logger,
                    message.Topic,
                    message.Tag,
                    message.MessageId,
                    message.BrokerName,
                    message.QueueId,
                    message.QueueOffset,
                    message.DeliveryAttempt,
                    outcome.ToString(),
                    duration,
                    EventBusPayloadJsonFormatter.Format(message.Body));
                return;
            }

            RemotingEventBusLogMessages.ConsumeDeadLetter(
                logger,
                message.Topic,
                message.Tag,
                message.MessageId,
                message.BrokerName,
                message.QueueId,
                message.QueueOffset,
                message.DeliveryAttempt,
                outcome.ToString(),
                duration);
        }
    }

    private static string FormatPayload(
        IIntegrationEventSerializer serializer,
        EventBusDispatchResult result,
        ReadOnlyMemory<byte> body) =>
        result.IntegrationEvent is null
            ? EventBusPayloadJsonFormatter.Format(body)
            : EventBusPayloadJsonFormatter.Format(serializer, result.IntegrationEvent, body)!;

    private static void Write(
        ILogger logger,
        EventBusLoggingSettings settings,
        LogLevel level,
        Action write)
    {
        if (!settings.Enabled)
        {
            return;
        }

        try
        {
            if (logger.IsEnabled(level))
            {
                write();
            }
        }
        catch (Exception)
        {
            // Logging is diagnostic and must not alter publish or delivery settlement.
        }
    }
}
