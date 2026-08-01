using System.Collections.Concurrent;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Support.Handlers;

internal sealed class RemotingDispatchRecorder
{
    private readonly ConcurrentQueue<string> _values = new();

    internal IReadOnlyList<string> Values => _values.ToArray();

    internal void Record(string value) => _values.Enqueue(value);
}
