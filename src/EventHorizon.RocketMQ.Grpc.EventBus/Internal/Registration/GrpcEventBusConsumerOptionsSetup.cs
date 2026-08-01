namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Registration;

internal sealed class GrpcEventBusConsumerOptionsSetup(
    GrpcEventBusConsumerOptionsMarker marker,
    IEventBusRoutePlan routePlan,
    GrpcEventBusConsumerConfiguration consumerConfiguration) :
    IConfigureNamedOptions<GrpcPushConsumerOptions>,
    IValidateOptions<GrpcPushConsumerOptions>
{
    private readonly GrpcEventBusConsumerOptionsMarker _marker =
        marker ?? throw new ArgumentNullException(nameof(marker));
    private readonly IEventBusRoutePlan _routePlan = routePlan ?? throw new ArgumentNullException(nameof(routePlan));
    private readonly GrpcEventBusConsumerConfiguration _consumerConfiguration =
        consumerConfiguration ?? throw new ArgumentNullException(nameof(consumerConfiguration));

    public void Configure(GrpcPushConsumerOptions options) => Configure(Options.DefaultName, options);

    public void Configure(string? name, GrpcPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!_marker.IsMarked(options))
        {
            return;
        }

        if (options.Subscriptions.Count != 0)
        {
            throw new InvalidOperationException(
                "EventBus owns all Push consumer subscriptions. Configure application Handlers instead of calling Subscribe.");
        }

        foreach (var subscription in _routePlan.Subscriptions)
        {
            options.Subscribe(subscription.Topic, new FilterExpression(subscription.FilterExpression));
        }

    }

    public ValidateOptionsResult Validate(string? name, GrpcPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!_marker.IsMarked(options))
        {
            return ValidateOptionsResult.Skip;
        }

        if (string.IsNullOrWhiteSpace(options.GroupName))
        {
            return ValidateOptionsResult.Fail("Consumer group name is required.");
        }

        if (!HasExpectedSubscriptions(options.Subscriptions))
        {
            return ValidateOptionsResult.Fail(
                "EventBus owns all Push consumer subscriptions; they cannot be changed after EventBus configuration.");
        }

        _consumerConfiguration.Set(options.GroupName, _routePlan);
        return ValidateOptionsResult.Success;
    }

    private bool HasExpectedSubscriptions(IReadOnlyDictionary<string, FilterExpression> subscriptions)
    {
        if (subscriptions.Count != _routePlan.Subscriptions.Count)
        {
            return false;
        }

        foreach (var expected in _routePlan.Subscriptions)
        {
            if (!subscriptions.TryGetValue(expected.Topic, out var actual) ||
                actual.Type != FilterExpressionType.Tag ||
                !string.Equals(actual.Expression, expected.FilterExpression, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
}
