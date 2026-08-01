using System.Reflection;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Consumer;

public sealed class RemotingEventBusDispatchOutcomeMapperTests
{
    [Theory]
    [InlineData("Success", ConsumeResult.Success)]
    [InlineData("Retry", ConsumeResult.Retry)]
    [InlineData("DeadLetter", ConsumeResult.DeadLetter)]
    public void Map_MapsEveryEventBusOutcomeExplicitly(string outcomeName, ConsumeResult expected)
    {
        var result = Map(CreateOutcome(outcomeName));

        Assert.Equal(expected, result);
    }

    [Fact]
    public void Map_RejectsAnUnknownOutcome()
    {
        var exception = Assert.Throws<TargetInvocationException>(() => Map(CreateOutcome(99)));

        Assert.IsType<ArgumentOutOfRangeException>(exception.InnerException);
        Assert.Equal("outcome", ((ArgumentOutOfRangeException)exception.InnerException!).ParamName);
    }

    private static ConsumeResult Map(object outcome) => (ConsumeResult)typeof(RemotingEventBusDispatchOutcomeMapper)
        .GetMethod("Map", BindingFlags.Static | BindingFlags.NonPublic)!
        .Invoke(null, [outcome])!;

    private static object CreateOutcome(string name) => Enum.Parse(GetOutcomeType(), name);

    private static object CreateOutcome(int value) => Enum.ToObject(GetOutcomeType(), value);

    private static Type GetOutcomeType() => typeof(IEventBus).Assembly.GetType(
        "EventHorizon.RocketMQ.EventBus.Internal.Dispatching.EventBusDispatchOutcome",
        throwOnError: true)!;
}
