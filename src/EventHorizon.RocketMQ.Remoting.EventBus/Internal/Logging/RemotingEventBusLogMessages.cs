namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Logging;

internal static partial class RemotingEventBusLogMessages
{
    [LoggerMessage(
        EventId = 2000,
        EventName = "EventBusPublishSucceededWithPayload",
        Level = LogLevel.Information,
        Message = "EventBus publish succeeded. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishSucceededWithPayload(
        ILogger logger,
        string topic,
        string? tag,
        string? messageId,
        TimeSpan duration,
        string? payload);

    [LoggerMessage(
        EventId = 2001,
        EventName = "EventBusPublishSucceeded",
        Level = LogLevel.Information,
        Message = "EventBus publish succeeded. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishSucceeded(
        ILogger logger,
        string topic,
        string? tag,
        string? messageId,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 2010,
        EventName = "EventBusPublishFailedWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus publish failed. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "TransportResult: {TransportResult}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishFailedWithPayload(
        ILogger logger,
        Exception exception,
        string topic,
        string? tag,
        string? messageId,
        string? transportResult,
        TimeSpan duration,
        string? payload);

    [LoggerMessage(
        EventId = 2011,
        EventName = "EventBusPublishFailed",
        Level = LogLevel.Error,
        Message = "EventBus publish failed. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "TransportResult: {TransportResult}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishFailed(
        ILogger logger,
        Exception exception,
        string topic,
        string? tag,
        string? messageId,
        string? transportResult,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 2100,
        EventName = "EventBusConsumeInvalidBatch",
        Level = LogLevel.Error,
        Message = "EventBus consume rejected an invalid batch. MessageCount: {MessageCount}. ExpectedMessageCount: 1",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeInvalidBatch(ILogger logger, int messageCount);

    [LoggerMessage(
        EventId = 2110,
        EventName = "EventBusConsumeUnhandledRetryWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus consume failed; retry requested. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeUnhandledRetryWithPayload(
        ILogger logger,
        Exception exception,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 2111,
        EventName = "EventBusConsumeUnhandledRetry",
        Level = LogLevel.Error,
        Message = "EventBus consume failed; retry requested. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeUnhandledRetry(
        ILogger logger,
        Exception exception,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 2120,
        EventName = "EventBusConsumeSucceededWithPayload",
        Level = LogLevel.Information,
        Message = "EventBus consume completed. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeSucceededWithPayload(
        ILogger logger,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 2121,
        EventName = "EventBusConsumeSucceeded",
        Level = LogLevel.Information,
        Message = "EventBus consume completed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeSucceeded(
        ILogger logger,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 2130,
        EventName = "EventBusConsumeRetryWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeRetryWithPayload(
        ILogger logger,
        Exception? exception,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 2131,
        EventName = "EventBusConsumeRetry",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeRetry(
        ILogger logger,
        Exception? exception,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 2140,
        EventName = "EventBusConsumeDeadLetterWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeDeadLetterWithPayload(
        ILogger logger,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 2141,
        EventName = "EventBusConsumeDeadLetter",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumeDeadLetter(
        ILogger logger,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 2142,
        EventName = "EventBusPayloadDeserializationFailed",
        Level = LogLevel.Error,
        Message = "EventBus payload deserialization failed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PayloadDeserializationFailed(
        ILogger logger,
        string topic,
        string? tag,
        string messageId,
        string? brokerName,
        int queueId,
        long queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 2200,
        EventName = "EventBusSubscriptionsMaterialized",
        Level = LogLevel.Information,
        Message = "EventBus subscriptions materialized. RegistrationName: {RegistrationName}. " +
            "ConsumerGroup: {ConsumerGroup}. HandlerCount: {HandlerCount}. SubscriptionCount: {SubscriptionCount}. " +
            "Subscriptions: {Subscriptions}",
        SkipEnabledCheck = true)]
    internal static partial void SubscriptionsMaterialized(
        ILogger logger,
        string registrationName,
        string consumerGroup,
        int handlerCount,
        int subscriptionCount,
        string subscriptions);
}
