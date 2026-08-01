namespace EventHorizon.RocketMQ.EventBus.Tests.Support.Events;

internal sealed class OrderSubmittedEvent : IntegrationEvent
{
    public OrderSubmittedEvent()
        : base("orders", "submitted")
    {
    }

    public string? OrderId { get; init; }

    public decimal Total { get; init; }
}

internal sealed class OrderCancelledEvent : IntegrationEvent
{
    public OrderCancelledEvent()
        : base("orders", "cancelled")
    {
    }
}

internal sealed class UntaggedOrderEvent : IntegrationEvent
{
    public UntaggedOrderEvent()
        : base("orders")
    {
    }
}

internal sealed class AccountCreatedEvent : IntegrationEvent
{
    public AccountCreatedEvent()
        : base("accounts", "created")
    {
    }
}

internal sealed class CaseVariantTagEvent : IntegrationEvent
{
    public CaseVariantTagEvent()
        : base("orders", "Submitted")
    {
    }
}

internal sealed class ExactWhitespaceRouteEvent : IntegrationEvent
{
    public ExactWhitespaceRouteEvent()
        : base(" orders ", " submitted ")
    {
    }
}

internal sealed class AmbiguousOrderSubmittedEvent : IntegrationEvent
{
    public AmbiguousOrderSubmittedEvent()
        : base("orders", "submitted")
    {
    }
}

internal sealed class SnapshotFirstEvent : IntegrationEvent
{
    public SnapshotFirstEvent()
        : base("snapshot", "first")
    {
    }
}

internal sealed class SnapshotSecondEvent : IntegrationEvent
{
    public SnapshotSecondEvent()
        : base("snapshot", "second")
    {
    }
}

internal sealed class DispatchEvent : IntegrationEvent
{
    public DispatchEvent()
        : base("dispatch", "received")
    {
    }

    public string? Value { get; init; }
}

internal sealed class UntaggedDispatchEvent : IntegrationEvent
{
    public UntaggedDispatchEvent()
        : base("dispatch")
    {
    }
}

internal sealed class NoDefaultConstructorEvent : IntegrationEvent
{
    public NoDefaultConstructorEvent(string value)
        : base("invalid", "no-default")
    {
        Value = value;
    }

    public string Value { get; }
}

internal sealed class ThrowingConstructorEvent : IntegrationEvent
{
    public ThrowingConstructorEvent()
        : base("invalid", "throwing")
    {
        throw new InvalidOperationException("Constructor failure.");
    }
}

internal sealed class UnstableRouteEvent : IntegrationEvent
{
    private static int _nextTag;

    public UnstableRouteEvent()
        : base("invalid", Interlocked.Increment(ref _nextTag).ToString())
    {
    }

    internal static void Reset() => _nextTag = 0;
}

internal sealed class BlankTopicEvent : IntegrationEvent
{
    public BlankTopicEvent()
        : base("", "tag")
    {
    }
}

internal sealed class BlankTagEvent : IntegrationEvent
{
    public BlankTagEvent()
        : base("topic", " ")
    {
    }
}

internal sealed class WildcardTagEvent : IntegrationEvent
{
    public WildcardTagEvent()
        : base("topic", "*")
    {
    }
}

internal sealed class ExpressionTagEvent : IntegrationEvent
{
    public ExpressionTagEvent()
        : base("topic", "one || two")
    {
    }
}

internal sealed class WireContractEvent : IntegrationEvent
{
    public WireContractEvent()
        : base("wire", "contract")
    {
    }

    public Guid OrderId { get; init; }

    public decimal Total { get; init; }

    public string? Note { get; init; }

    public int Quantity { get; init; }
}

internal sealed class DepthContractEvent : IntegrationEvent
{
    public DepthContractEvent()
        : base("wire", "depth")
    {
    }

    public object? Payload { get; init; }
}
