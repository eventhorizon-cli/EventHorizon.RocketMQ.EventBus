using Xunit;

namespace EventHorizon.RocketMQ.EventBus.Tests.Contracts;

public sealed class IntegrationEventTests
{
    [Fact]
    public void Constructor_PreservesExactRouteMetadata()
    {
        var integrationEvent = new OrderSubmittedEvent();

        Assert.Equal("orders", integrationEvent.Topic);
        Assert.Equal("submitted", integrationEvent.Tag);
    }

    [Fact]
    public void Constructor_AllowsAnUntaggedEvent()
    {
        var integrationEvent = new UntaggedOrderEvent();

        Assert.Equal("orders", integrationEvent.Topic);
        Assert.Null(integrationEvent.Tag);
    }

    [Fact]
    public void Constructor_RejectsBlankTopic()
    {
        Assert.Throws<ArgumentException>(() => new BlankTopicEvent());
    }

    [Fact]
    public void Constructor_RejectsBlankTag()
    {
        Assert.Throws<ArgumentException>(() => new BlankTagEvent());
    }

    [Fact]
    public void Constructor_RejectsWildcardTag()
    {
        Assert.Throws<ArgumentException>(() => new WildcardTagEvent());
    }

    [Fact]
    public void Constructor_RejectsTagExpression()
    {
        Assert.Throws<ArgumentException>(() => new ExpressionTagEvent());
    }
}
