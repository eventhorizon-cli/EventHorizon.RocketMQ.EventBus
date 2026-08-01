using EventHorizon.RocketMQ.EventBus.Events;
using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;

namespace EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests.Support;

internal sealed class GrpcTaggedIntegrationEvent : IntegrationEvent
{
    public GrpcTaggedIntegrationEvent()
        : base(RocketMQGrpcClusterFixture.Topic, "eventbus-tagged")
    {
    }

    public Guid DeliveryId { get; init; }
}

internal sealed class GrpcUntaggedIntegrationEvent : IntegrationEvent
{
    public GrpcUntaggedIntegrationEvent()
        : base(RocketMQGrpcClusterFixture.Topic)
    {
    }

    public Guid DeliveryId { get; init; }
}
