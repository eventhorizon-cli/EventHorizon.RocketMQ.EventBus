using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Events;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Handlers;

internal sealed class OrderSubmittedHandler(ILogger<OrderSubmittedHandler> logger)
    : IIntegrationEventBusHandler<OrderSubmittedIntegrationEvent>
{
    public Task HandleAsync(
        OrderSubmittedIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation(
            "Handled tagged order event {OrderId} with total {Total}",
            integrationEvent.OrderId,
            integrationEvent.Total);
        return Task.CompletedTask;
    }
}
