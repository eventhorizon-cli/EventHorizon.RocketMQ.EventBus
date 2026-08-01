namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Logging;

internal static class GrpcEventBusLoggerExtensions
{
    internal static void LogEventBusPublishSucceeded(
        this ILogger logger,
        EventBusLoggingSettings settings,
        IntegrationEvent integrationEvent,
        string? messageId,
        TimeSpan duration,
        IIntegrationEventSerializer serializer,
        ReadOnlyMemory<byte> body) =>
        Write(logger, settings, LogLevel.Information, () =>
        {
            if (settings.IncludePayload)
            {
                GrpcEventBusLogMessages.PublishCompletedWithPayload(
                    logger,
                    integrationEvent.Topic,
                    integrationEvent.Tag,
                    messageId,
                    duration,
                    EventBusPayloadJsonFormatter.Format(serializer, integrationEvent, body));
                return;
            }

            GrpcEventBusLogMessages.PublishCompleted(
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
        TimeSpan duration,
        IIntegrationEventSerializer? serializer,
        ReadOnlyMemory<byte>? body) =>
        Write(logger, settings, LogLevel.Error, () =>
        {
            if (settings.IncludePayload)
            {
                GrpcEventBusLogMessages.PublishFailedWithPayload(
                    logger,
                    exception,
                    exception.Topic,
                    exception.Tag,
                    duration,
                    EventBusPayloadJsonFormatter.Format(serializer, integrationEvent, body));
                return;
            }

            GrpcEventBusLogMessages.PublishFailed(
                logger,
                exception,
                exception.Topic,
                exception.Tag,
                duration);
        });

    internal static void LogEventBusUnhandledRetry(
        this ILogger logger,
        EventBusLoggingSettings settings,
        Exception exception,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        TimeSpan duration,
        ReadOnlyMemory<byte> body) =>
        Write(logger, settings, LogLevel.Error, () =>
        {
            if (settings.IncludePayload)
            {
                GrpcEventBusLogMessages.ConsumerUnexpectedRetryWithPayload(
                    logger,
                    exception,
                    topic,
                    tag,
                    messageId,
                    brokerName,
                    queueId,
                    queueOffset,
                    deliveryAttempt,
                    nameof(EventBusDispatchOutcome.Retry),
                    duration,
                    EventBusPayloadJsonFormatter.Format(body));
                return;
            }

            GrpcEventBusLogMessages.ConsumerUnexpectedRetry(
                logger,
                exception,
                topic,
                tag,
                messageId,
                brokerName,
                queueId,
                queueOffset,
                deliveryAttempt,
                nameof(EventBusDispatchOutcome.Retry),
                duration);
        });

    internal static void LogEventBusConsumeCompleted(
        this ILogger logger,
        EventBusLoggingSettings settings,
        IIntegrationEventSerializer serializer,
        EventBusDispatchResult result,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        TimeSpan duration,
        ReadOnlyMemory<byte> body)
    {
        var level = result.Outcome == EventBusDispatchOutcome.Success ? LogLevel.Information : LogLevel.Error;
        Write(logger, settings, level, () => WriteConsumerOutcome(
            logger,
            settings,
            serializer,
            result,
            topic,
            tag,
            messageId,
            brokerName,
            queueId,
            queueOffset,
            deliveryAttempt,
            duration,
            body));
    }

    internal static void LogEventBusSubscriptionsMaterialized(
        this ILogger logger,
        EventBusLoggingSettings settings,
        string registrationName,
        string consumerGroup,
        int handlerCount,
        IReadOnlyList<GrpcEventBusSubscription> subscriptions) =>
        Write(logger, settings, LogLevel.Information, () => GrpcEventBusLogMessages.SubscriptionsMaterialized(
            logger,
            registrationName,
            consumerGroup,
            handlerCount,
            subscriptions.Count,
            string.Join("; ", subscriptions.Select(static subscription =>
                $"{subscription.Topic}: {subscription.FilterExpression}"))));

    private static void WriteConsumerOutcome(
        ILogger logger,
        EventBusLoggingSettings settings,
        IIntegrationEventSerializer serializer,
        EventBusDispatchResult result,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        TimeSpan duration,
        ReadOnlyMemory<byte> body)
    {
        switch (result.Outcome)
        {
            case EventBusDispatchOutcome.Success:
                WriteSuccessfulConsumerOutcome();
                return;

            case EventBusDispatchOutcome.Retry:
                WriteRetryConsumerOutcome();
                return;

            case EventBusDispatchOutcome.DeadLetter:
                WriteDeadLetterConsumerOutcome();
                return;
        }

        void WriteSuccessfulConsumerOutcome()
        {
            if (settings.IncludePayload)
            {
                GrpcEventBusLogMessages.ConsumerSucceededWithPayload(
                    logger,
                    topic,
                    tag,
                    messageId,
                    brokerName,
                    queueId,
                    queueOffset,
                    deliveryAttempt,
                    nameof(EventBusDispatchOutcome.Success),
                    duration,
                    FormatPayload(serializer, result, body));
                return;
            }

            GrpcEventBusLogMessages.ConsumerSucceeded(
                logger,
                topic,
                tag,
                messageId,
                brokerName,
                queueId,
                queueOffset,
                deliveryAttempt,
                nameof(EventBusDispatchOutcome.Success),
                duration);
        }

        void WriteRetryConsumerOutcome()
        {
            if (settings.IncludePayload)
            {
                GrpcEventBusLogMessages.ConsumerRetryWithPayload(
                    logger,
                    result.Exception,
                    topic,
                    tag,
                    messageId,
                    brokerName,
                    queueId,
                    queueOffset,
                    deliveryAttempt,
                    nameof(EventBusDispatchOutcome.Retry),
                    duration,
                    FormatPayload(serializer, result, body));
                return;
            }

            GrpcEventBusLogMessages.ConsumerRetry(
                logger,
                result.Exception,
                topic,
                tag,
                messageId,
                brokerName,
                queueId,
                queueOffset,
                deliveryAttempt,
                nameof(EventBusDispatchOutcome.Retry),
                duration);
        }

        void WriteDeadLetterConsumerOutcome()
        {
            if (result.DeserializationFailed)
            {
                GrpcEventBusLogMessages.ConsumerDeserializationFailed(
                    logger,
                    topic,
                    tag,
                    messageId,
                    brokerName,
                    queueId,
                    queueOffset,
                    deliveryAttempt,
                    nameof(EventBusDispatchOutcome.DeadLetter),
                    duration);
                return;
            }

            if (settings.IncludePayload)
            {
                GrpcEventBusLogMessages.ConsumerDeadLetterWithPayload(
                    logger,
                    topic,
                    tag,
                    messageId,
                    brokerName,
                    queueId,
                    queueOffset,
                    deliveryAttempt,
                    nameof(EventBusDispatchOutcome.DeadLetter),
                    duration,
                    EventBusPayloadJsonFormatter.Format(body));
                return;
            }

            GrpcEventBusLogMessages.ConsumerDeadLetter(
                logger,
                topic,
                tag,
                messageId,
                brokerName,
                queueId,
                queueOffset,
                deliveryAttempt,
                nameof(EventBusDispatchOutcome.DeadLetter),
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
