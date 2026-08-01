using EventHorizon.RocketMQ.EventBus.Events;
using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;

namespace EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.Support;

internal sealed class RemotingTaggedIntegrationEvent : IntegrationEvent
{
    public RemotingTaggedIntegrationEvent()
        : base(RocketMQRemotingClusterFixture.Topic, "eventbus-tagged")
    {
    }

    public Guid DeliveryId { get; init; }
}

internal sealed class RemotingUntaggedIntegrationEvent : IntegrationEvent
{
    public RemotingUntaggedIntegrationEvent()
        : base(RocketMQRemotingClusterFixture.Topic)
    {
    }

    public Guid DeliveryId { get; init; }
}
