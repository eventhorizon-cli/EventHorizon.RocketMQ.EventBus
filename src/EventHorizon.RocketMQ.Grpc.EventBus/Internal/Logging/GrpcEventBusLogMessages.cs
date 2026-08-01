namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Logging;

internal static partial class GrpcEventBusLogMessages
{
    [LoggerMessage(
        EventId = 1000,
        EventName = "EventBusPublishSucceededWithPayload",
        Level = LogLevel.Information,
        Message = "EventBus publish succeeded. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishCompletedWithPayload(
        ILogger logger,
        string topic,
        string? tag,
        string? messageId,
        TimeSpan duration,
        string? payload);

    [LoggerMessage(
        EventId = 1001,
        EventName = "EventBusPublishSucceeded",
        Level = LogLevel.Information,
        Message = "EventBus publish succeeded. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishCompleted(
        ILogger logger,
        string topic,
        string? tag,
        string? messageId,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 1010,
        EventName = "EventBusPublishFailedWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus publish failed. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishFailedWithPayload(
        ILogger logger,
        Exception exception,
        string topic,
        string? tag,
        TimeSpan duration,
        string? payload);

    [LoggerMessage(
        EventId = 1011,
        EventName = "EventBusPublishFailed",
        Level = LogLevel.Error,
        Message = "EventBus publish failed. Topic: {Topic}. Tag: {Tag}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void PublishFailed(
        ILogger logger,
        Exception exception,
        string topic,
        string? tag,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 1110,
        EventName = "EventBusConsumeUnhandledRetryWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus consume failed; retry requested. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerUnexpectedRetryWithPayload(
        ILogger logger,
        Exception exception,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 1111,
        EventName = "EventBusConsumeUnhandledRetry",
        Level = LogLevel.Error,
        Message = "EventBus consume failed; retry requested. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerUnexpectedRetry(
        ILogger logger,
        Exception exception,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 1120,
        EventName = "EventBusConsumeSucceededWithPayload",
        Level = LogLevel.Information,
        Message = "EventBus consume completed. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerSucceededWithPayload(
        ILogger logger,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 1121,
        EventName = "EventBusConsumeSucceeded",
        Level = LogLevel.Information,
        Message = "EventBus consume completed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerSucceeded(
        ILogger logger,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 1130,
        EventName = "EventBusConsumeRetryWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerRetryWithPayload(
        ILogger logger,
        Exception? exception,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 1131,
        EventName = "EventBusConsumeRetry",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerRetry(
        ILogger logger,
        Exception? exception,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 1140,
        EventName = "EventBusConsumeDeadLetterWithPayload",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Payload: {Payload}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerDeadLetterWithPayload(
        ILogger logger,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration,
        string payload);

    [LoggerMessage(
        EventId = 1141,
        EventName = "EventBusConsumeDeadLetter",
        Level = LogLevel.Error,
        Message = "EventBus consume completed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. MessageId: {MessageId}. " +
            "BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerDeadLetter(
        ILogger logger,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 1142,
        EventName = "EventBusPayloadDeserializationFailed",
        Level = LogLevel.Error,
        Message = "EventBus payload deserialization failed. Outcome: {Outcome}. Topic: {Topic}. Tag: {Tag}. " +
            "MessageId: {MessageId}. BrokerName: {BrokerName}. QueueId: {QueueId}. QueueOffset: {QueueOffset}. " +
            "DeliveryAttempt: {DeliveryAttempt}. Duration: {Duration}",
        SkipEnabledCheck = true)]
    internal static partial void ConsumerDeserializationFailed(
        ILogger logger,
        string? topic,
        string? tag,
        string? messageId,
        string? brokerName,
        int? queueId,
        long? queueOffset,
        int deliveryAttempt,
        string outcome,
        TimeSpan duration);

    [LoggerMessage(
        EventId = 1200,
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
