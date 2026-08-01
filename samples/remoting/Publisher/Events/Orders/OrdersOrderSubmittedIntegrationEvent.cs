using EventHorizon.RocketMQ.EventBus.Events;

namespace EventHorizon.RocketMQ.EventBus.Samples.Remoting.Publisher.Events.Orders;

public sealed class OrdersOrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrdersOrderSubmittedIntegrationEvent()
        : base("eventbus-orders", "order-submitted-named")
    {
    }

    public Guid OrderId { get; init; }

    public decimal Total { get; init; }
}
