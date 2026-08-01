using System.Collections.Concurrent;

namespace EventHorizon.RocketMQ.EventBus.Tests.Support.Dispatching;

internal sealed class DispatchRecorder
{
    private readonly ConcurrentQueue<string> _entries = new();

    internal IReadOnlyList<string> Entries => _entries.ToArray();

    internal void Add(string value) => _entries.Enqueue(value);
}

internal sealed class DispatchFirstHandler(DispatchRecorder recorder) : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Add("first");
        return Task.CompletedTask;
    }
}

internal sealed class DispatchSecondHandler(DispatchRecorder recorder) : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Add("second");
        return Task.CompletedTask;
    }
}

internal sealed class UntaggedDispatchHandler(DispatchRecorder recorder)
    : IIntegrationEventBusHandler<UntaggedDispatchEvent>
{
    public Task HandleAsync(UntaggedDispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Add("untagged");
        return Task.CompletedTask;
    }
}

internal sealed class ThrowingDispatchHandler(DispatchRecorder recorder) : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Add("throwing");
        throw new InvalidOperationException("Handler failure.");
    }
}

internal sealed class AfterThrowingDispatchHandler(DispatchRecorder recorder)
    : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Add("after");
        return Task.CompletedTask;
    }
}

internal sealed class CancellingDispatchHandler : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default) =>
        Task.FromCanceled(cancellationToken);
}

internal sealed class MissingDispatchDependency;

internal sealed class MissingDependencyDispatchHandler(MissingDispatchDependency dependency)
    : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        _ = dependency;
        return Task.CompletedTask;
    }
}

internal sealed class ScopedProbe : IAsyncDisposable
{
    internal Guid InstanceId { get; } = Guid.NewGuid();

    internal bool Disposed { get; private set; }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

internal sealed class ScopedDispatchFirstHandler(DispatchRecorder recorder, ScopedProbe probe)
    : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Add($"first:{probe.InstanceId}");
        return Task.CompletedTask;
    }
}

internal sealed class ScopedDispatchSecondHandler(DispatchRecorder recorder, ScopedProbe probe)
    : IIntegrationEventBusHandler<DispatchEvent>
{
    public Task HandleAsync(DispatchEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        recorder.Add($"second:{probe.InstanceId}");
        return Task.CompletedTask;
    }
}
