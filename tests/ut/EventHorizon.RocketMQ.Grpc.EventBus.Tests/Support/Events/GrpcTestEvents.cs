namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Support.Events;

internal sealed class OrderCreatedEvent : IntegrationEvent
{
    public OrderCreatedEvent()
        : base("orders", "created")
    {
    }

    public string? Value { get; init; }
}

internal sealed class OrderPlacedEvent : IntegrationEvent
{
    public OrderPlacedEvent()
        : base("orders", "placed")
    {
    }
}

internal sealed class BillingCapturedEvent : IntegrationEvent
{
    public BillingCapturedEvent()
        : base("billing", "captured")
    {
    }
}

internal sealed class GrpcUntaggedEvent : IntegrationEvent
{
    public GrpcUntaggedEvent()
        : base("orders")
    {
    }

    public string? Value { get; init; }
}

internal sealed class GrpcDispatchEvent : IntegrationEvent
{
    public GrpcDispatchEvent()
        : base("dispatch", "received")
    {
    }

    public string? Value { get; init; }
}
