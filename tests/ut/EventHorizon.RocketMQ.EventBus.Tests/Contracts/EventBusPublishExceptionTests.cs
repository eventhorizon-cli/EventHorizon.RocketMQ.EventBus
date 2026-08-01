using Xunit;

namespace EventHorizon.RocketMQ.EventBus.Tests.Contracts;

public sealed class EventBusPublishExceptionTests
{
    [Fact]
    public void Constructor_PreservesPublishMetadataAndTheOriginalException()
    {
        var innerException = new InvalidOperationException("Transport failed.");
        var exception = new EventBusPublishException(
            typeof(OrderSubmittedEvent),
            "orders",
            "submitted",
            "orders-publisher",
            "SendFailed",
            innerException);

        Assert.Equal(typeof(OrderSubmittedEvent), exception.IntegrationEventType);
        Assert.Equal("orders", exception.Topic);
        Assert.Equal("submitted", exception.Tag);
        Assert.Equal("orders-publisher", exception.RegistrationName);
        Assert.Equal("SendFailed", exception.TransportResult);
        Assert.Same(innerException, exception.InnerException);
        Assert.DoesNotContain("body", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Constructor_PreservesAnUntaggedPublishWithoutAnAmbiguousEmptyValue()
    {
        var exception = new EventBusPublishException(
            typeof(UntaggedOrderEvent),
            "orders",
            null,
            null);

        Assert.Null(exception.Tag);
        Assert.Contains("<none>", exception.Message, StringComparison.Ordinal);
    }
}
