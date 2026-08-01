namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Consumer;

internal static class GrpcEventBusConsumeResultMapper
{
    internal static ConsumeResult Map(EventBusDispatchOutcome outcome) => outcome switch
    {
        EventBusDispatchOutcome.Success => ConsumeResult.Success,
        EventBusDispatchOutcome.Retry => ConsumeResult.Retry,
        EventBusDispatchOutcome.DeadLetter => ConsumeResult.DeadLetter,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unknown EventBus dispatch outcome."),
    };
}
