namespace EventHorizon.RocketMQ.EventBus.Internal.Dispatching;

internal sealed record EventBusConsumptionSettings(bool SkipDeserializationFailures);
