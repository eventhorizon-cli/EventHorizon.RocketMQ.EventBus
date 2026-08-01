namespace EventHorizon.RocketMQ.EventBus.Internal.Logging;

internal sealed record EventBusLoggingSettings(bool Enabled, bool IncludePayload);
