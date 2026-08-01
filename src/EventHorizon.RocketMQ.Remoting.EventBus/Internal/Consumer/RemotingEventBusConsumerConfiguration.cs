using System.Runtime.CompilerServices;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Consumer;

internal sealed class RemotingEventBusConsumerConfiguration(
    Action<RemotingPushConsumerOptions>? configureConsumer)
{
    private readonly Action<RemotingPushConsumerOptions>? _configureConsumer = configureConsumer;
    private readonly ConditionalWeakTable<RemotingPushConsumerOptions, object> _options = new();

    internal void Configure(RemotingPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _configureConsumer?.Invoke(options);
        _options.GetValue(options, static _ => new object());
    }

    internal bool Owns(RemotingPushConsumerOptions options) => _options.TryGetValue(options, out _);
}
