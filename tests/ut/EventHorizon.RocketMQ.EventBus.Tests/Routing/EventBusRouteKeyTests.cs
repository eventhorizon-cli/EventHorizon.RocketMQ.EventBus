using Xunit;

namespace EventHorizon.RocketMQ.EventBus.Tests.Routing;

public sealed class EventBusRouteKeyTests
{
    [Fact]
    public void DictionaryLookup_UsesOrdinalTopicAndTagEquality()
    {
        var routes = new Dictionary<EventBusRouteKey, string>
        {
            [new EventBusRouteKey("orders", "submitted")] = "route",
        };

        Assert.True(routes.TryGetValue(new EventBusRouteKey("orders", "submitted"), out var route));
        Assert.Equal("route", route);
        Assert.False(routes.ContainsKey(new EventBusRouteKey("Orders", "submitted")));
        Assert.False(routes.ContainsKey(new EventBusRouteKey("orders", "Submitted")));
    }

    [Fact]
    public void EqualityOperators_AgreeWithStronglyTypedEqualityAndHashCodes()
    {
        var left = new EventBusRouteKey("orders", "submitted");
        var right = new EventBusRouteKey("orders", "submitted");

        Assert.True(left.Equals(right));
        Assert.True(left == right);
        Assert.False(left != right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
    }

    [Fact]
    public void DictionaryLookup_DistinguishesAnUntaggedRouteFromTaggedRoutes()
    {
        var routes = new Dictionary<EventBusRouteKey, string>
        {
            [new EventBusRouteKey("orders", null)] = "untagged",
            [new EventBusRouteKey("orders", "submitted")] = "tagged",
        };

        Assert.Equal("untagged", routes[new EventBusRouteKey("orders", null)]);
        Assert.Equal("tagged", routes[new EventBusRouteKey("orders", "submitted")]);
    }
}
