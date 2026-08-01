namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Support.Handlers;

internal sealed class GrpcDispatchRecorder
{
    private readonly ConcurrentQueue<string> _values = new();

    internal IReadOnlyList<string> Values => _values.ToArray();

    internal void Record(string value) => _values.Enqueue(value);
}
