namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Consumer;

internal static class GrpcEventBusConsumeResultMapper
{
    internal static ConsumeResult Map(EventBusDispatchOutcome outcome) => outcome switch
    {
        EventBusDispatchOutcome.Success => ConsumeResult.Success,
        EventBusDispatchOutcome.Retry => ConsumeResult.Failure,
        EventBusDispatchOutcome.DeadLetter => ConsumeResult.Failure,
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unknown EventBus dispatch outcome."),
    };
}
