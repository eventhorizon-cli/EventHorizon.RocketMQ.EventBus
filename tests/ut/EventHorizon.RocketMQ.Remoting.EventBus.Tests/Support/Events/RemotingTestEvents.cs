namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Support.Events;

internal sealed class RemotingTestEvent : IntegrationEvent
{
    public RemotingTestEvent()
        : base("orders", "submitted")
    {
    }

    public string? Value { get; init; }
}

internal sealed class RemotingSecondTestEvent : IntegrationEvent
{
    public RemotingSecondTestEvent()
        : base("orders", "cancelled")
    {
    }
}

internal sealed class RemotingUntaggedTestEvent : IntegrationEvent
{
    public RemotingUntaggedTestEvent()
        : base("orders")
    {
    }

    public string? Value { get; init; }
}
