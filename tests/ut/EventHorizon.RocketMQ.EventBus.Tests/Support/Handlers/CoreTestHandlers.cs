namespace EventHorizon.RocketMQ.EventBus.Tests.Support.Handlers;

internal sealed class SubmittedFirstHandler : IIntegrationEventBusHandler<OrderSubmittedEvent>
{
    public Task HandleAsync(OrderSubmittedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class SubmittedSecondHandler : IIntegrationEventBusHandler<OrderSubmittedEvent>
{
    public Task HandleAsync(OrderSubmittedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class CancelledHandler : IIntegrationEventBusHandler<OrderCancelledEvent>
{
    public Task HandleAsync(OrderCancelledEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class UntaggedOrderHandler : IIntegrationEventBusHandler<UntaggedOrderEvent>
{
    public Task HandleAsync(UntaggedOrderEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class AccountCreatedHandler : IIntegrationEventBusHandler<AccountCreatedEvent>
{
    public Task HandleAsync(AccountCreatedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class CaseVariantTagHandler : IIntegrationEventBusHandler<CaseVariantTagEvent>
{
    public Task HandleAsync(CaseVariantTagEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class ExactWhitespaceRouteHandler : IIntegrationEventBusHandler<ExactWhitespaceRouteEvent>
{
    public Task HandleAsync(ExactWhitespaceRouteEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class MultiEventHandler :
    IIntegrationEventBusHandler<OrderSubmittedEvent>,
    IIntegrationEventBusHandler<OrderCancelledEvent>
{
    public Task HandleAsync(OrderSubmittedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    public Task HandleAsync(OrderCancelledEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class AmbiguousRouteHandler : IIntegrationEventBusHandler<AmbiguousOrderSubmittedEvent>
{
    public Task HandleAsync(AmbiguousOrderSubmittedEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class NoDefaultConstructorHandler : IIntegrationEventBusHandler<NoDefaultConstructorEvent>
{
    public Task HandleAsync(NoDefaultConstructorEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class ThrowingConstructorHandler : IIntegrationEventBusHandler<ThrowingConstructorEvent>
{
    public Task HandleAsync(ThrowingConstructorEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class UnstableRouteHandler : IIntegrationEventBusHandler<UnstableRouteEvent>
{
    public Task HandleAsync(UnstableRouteEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class SnapshotFirstHandler : IIntegrationEventBusHandler<SnapshotFirstEvent>
{
    public Task HandleAsync(SnapshotFirstEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class SnapshotSecondHandler : IIntegrationEventBusHandler<SnapshotSecondEvent>
{
    public Task HandleAsync(SnapshotSecondEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}
