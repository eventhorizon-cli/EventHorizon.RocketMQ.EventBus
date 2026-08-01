using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Events.Orders;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Handlers.Orders;

internal sealed class OrdersOrderSubmittedHandler(ILogger<OrdersOrderSubmittedHandler> logger)
    : IIntegrationEventBusHandler<OrdersOrderSubmittedIntegrationEvent>
{
    public Task HandleAsync(
        OrdersOrderSubmittedIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Handled named tagged order event {OrderId}", integrationEvent.OrderId);
        return Task.CompletedTask;
    }
}
