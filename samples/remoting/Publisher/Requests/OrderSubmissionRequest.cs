namespace EventHorizon.RocketMQ.EventBus.Samples.Remoting.Publisher.Requests;

public sealed class OrderSubmissionRequest
{
    public Guid OrderId { get; init; }

    public decimal Total { get; init; }
}
