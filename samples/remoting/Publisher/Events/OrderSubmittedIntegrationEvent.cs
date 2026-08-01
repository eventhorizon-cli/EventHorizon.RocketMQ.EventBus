using EventHorizon.RocketMQ.EventBus.Events;

namespace EventHorizon.RocketMQ.EventBus.Samples.Remoting.Publisher.Events;

public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("eventbus-orders", "order-submitted")
    {
    }

    public Guid OrderId { get; init; }

    public decimal Total { get; init; }
}
