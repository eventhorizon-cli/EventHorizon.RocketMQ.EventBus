using System.Runtime.CompilerServices;

namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Registration;

internal sealed class GrpcEventBusConsumerOptionsMarker
{
    private readonly ConditionalWeakTable<GrpcPushConsumerOptions, object> _options = new();

    internal void Mark(GrpcPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options.GetValue(options, static _ => new object());
    }

    internal bool IsMarked(GrpcPushConsumerOptions options) => _options.TryGetValue(options, out _);
}
