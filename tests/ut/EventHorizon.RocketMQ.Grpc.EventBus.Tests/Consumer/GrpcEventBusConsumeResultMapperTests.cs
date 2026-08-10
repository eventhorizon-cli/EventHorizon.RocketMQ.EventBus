namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Consumer;

public sealed class GrpcEventBusConsumeResultMapperTests
{
    [Theory]
    [InlineData("Success", true)]
    [InlineData("Retry", false)]
    public void Map_KnownEventBusOutcome_ReturnsSupportedGrpcResult(
        string outcomeName,
        bool succeeds)
    {
        var outcomeType = typeof(IEventBus).Assembly.GetType(
            "EventHorizon.RocketMQ.EventBus.Internal.Dispatching.EventBusDispatchOutcome",
            throwOnError: true)!;
        var mapper = typeof(GrpcEventBusConsumeResultMapper).GetMethod(
            "Map",
            BindingFlags.Static | BindingFlags.NonPublic)!;
        var outcome = Enum.Parse(outcomeType, outcomeName);

        var result = (ConsumeResult)mapper.Invoke(null, [outcome])!;
        var expected = succeeds ? ConsumeResult.Success : ConsumeResult.Failure;

        Assert.Same(expected, result);
    }

    [Fact]
    public void EventBusDispatchOutcome_EnumNames_ContainOnlySuccessAndRetry()
    {
        var outcomeType = typeof(IEventBus).Assembly.GetType(
            "EventHorizon.RocketMQ.EventBus.Internal.Dispatching.EventBusDispatchOutcome",
            throwOnError: true)!;

        Assert.Equal(["Success", "Retry"], Enum.GetNames(outcomeType));
    }

    [Fact]
    public void Map_UnknownEventBusOutcome_ThrowsArgumentOutOfRangeException()
    {
        var outcomeType = typeof(IEventBus).Assembly.GetType(
            "EventHorizon.RocketMQ.EventBus.Internal.Dispatching.EventBusDispatchOutcome",
            throwOnError: true)!;
        var mapper = typeof(GrpcEventBusConsumeResultMapper).GetMethod(
            "Map",
            BindingFlags.Static | BindingFlags.NonPublic)!;
        var unknownOutcome = Enum.ToObject(outcomeType, 99);

        var exception = Assert.Throws<TargetInvocationException>(() => mapper.Invoke(null, [unknownOutcome]));

        Assert.IsType<ArgumentOutOfRangeException>(exception.InnerException);
    }
}
