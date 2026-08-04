using EventHorizon.RocketMQ.EventBus.Events;
using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;

namespace EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.Support;

internal sealed class RemotingTaggedIntegrationEvent : IntegrationEvent
{
    public RemotingTaggedIntegrationEvent()
        : base(RocketMQRemotingClusterFixture.PullTopic, "eventbus-tagged")
    {
    }

    public Guid DeliveryId { get; init; }
}

internal sealed class RemotingUntaggedIntegrationEvent : IntegrationEvent
{
    public RemotingUntaggedIntegrationEvent()
        : base(RocketMQRemotingClusterFixture.PullTopic)
    {
    }

    public Guid DeliveryId { get; init; }
}

internal sealed class RemotingPopIntegrationEvent : IntegrationEvent
{
    public RemotingPopIntegrationEvent()
        : base(RocketMQRemotingClusterFixture.PopTopic, "eventbus-pop")
    {
    }

    public Guid DeliveryId { get; init; }
}
