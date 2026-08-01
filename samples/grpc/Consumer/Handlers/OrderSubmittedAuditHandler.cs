using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Events;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Handlers;

internal sealed class OrderSubmittedAuditHandler(ILogger<OrderSubmittedAuditHandler> logger)
    : IIntegrationEventBusHandler<OrderSubmittedIntegrationEvent>
{
    public Task HandleAsync(
        OrderSubmittedIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Audited tagged order event {OrderId}", integrationEvent.OrderId);
        return Task.CompletedTask;
    }
}
