namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Registration;

internal sealed record GrpcEventBusSubscription(string Topic, string FilterExpression);
