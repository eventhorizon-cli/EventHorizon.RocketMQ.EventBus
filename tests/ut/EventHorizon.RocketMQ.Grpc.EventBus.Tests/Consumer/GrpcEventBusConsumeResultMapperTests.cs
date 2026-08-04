namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Consumer;

public sealed class GrpcEventBusConsumeResultMapperTests
{
    [Theory]
    [InlineData("Success", ConsumeResult.Success)]
    [InlineData("Retry", ConsumeResult.Failure)]
    [InlineData("DeadLetter", ConsumeResult.Failure)]
    public void Map_KnownEventBusOutcome_ReturnsSupportedGrpcResult(
        string outcomeName,
        ConsumeResult expected)
    {
        var outcomeType = typeof(IEventBus).Assembly.GetType(
            "EventHorizon.RocketMQ.EventBus.Internal.Dispatching.EventBusDispatchOutcome",
            throwOnError: true)!;
        var mapper = typeof(GrpcEventBusConsumeResultMapper).GetMethod(
            "Map",
            BindingFlags.Static | BindingFlags.NonPublic)!;
        var outcome = Enum.Parse(outcomeType, outcomeName);

        var result = (ConsumeResult)mapper.Invoke(null, [outcome])!;

        Assert.Equal(expected, result);
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
