namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Registration;

internal sealed record GrpcEventBusConsumerConfigurationSnapshot(
    string GroupName,
    int HandlerCount,
    IReadOnlyList<GrpcEventBusSubscription> Subscriptions);
