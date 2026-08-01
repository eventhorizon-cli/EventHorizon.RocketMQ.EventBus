namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Support.Handlers;

internal sealed class RemotingTestHandler(RemotingDispatchRecorder recorder)
    : IIntegrationEventBusHandler<RemotingTestEvent>
{
    public Task HandleAsync(RemotingTestEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Record(integrationEvent.Value ?? string.Empty);
        return Task.CompletedTask;
    }
}

internal sealed class RemotingSecondTestHandler : IIntegrationEventBusHandler<RemotingSecondTestEvent>
{
    public Task HandleAsync(RemotingSecondTestEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}

internal sealed class RemotingUntaggedTestHandler(RemotingDispatchRecorder recorder)
    : IIntegrationEventBusHandler<RemotingUntaggedTestEvent>
{
    public Task HandleAsync(RemotingUntaggedTestEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Record(integrationEvent.Value ?? string.Empty);
        return Task.CompletedTask;
    }
}

internal sealed class ThrowingRemotingTestHandler : IIntegrationEventBusHandler<RemotingTestEvent>
{
    public Task HandleAsync(RemotingTestEvent integrationEvent, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Expected Handler failure.");
}

internal sealed class CancellingRemotingTestHandler : IIntegrationEventBusHandler<RemotingTestEvent>
{
    public Task HandleAsync(RemotingTestEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.FromCanceled(cancellationToken);
}
