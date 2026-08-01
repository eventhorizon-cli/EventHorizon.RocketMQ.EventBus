namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Consumer;

internal sealed class RemotingEventBusConsumerOptionsSetup(
    RemotingEventBusConsumerConfiguration consumerConfiguration,
    IEventBusRoutePlan routePlan,
    RemotingEventBusSubscriptionSummary subscriptionSummary) :
    IConfigureNamedOptions<RemotingPushConsumerOptions>,
    IValidateOptions<RemotingPushConsumerOptions>
{
    private readonly RemotingEventBusConsumerConfiguration _consumerConfiguration =
        consumerConfiguration ?? throw new ArgumentNullException(nameof(consumerConfiguration));
    private readonly IEventBusRoutePlan _routePlan = routePlan ?? throw new ArgumentNullException(nameof(routePlan));
    private readonly RemotingEventBusSubscriptionSummary _subscriptionSummary =
        subscriptionSummary ?? throw new ArgumentNullException(nameof(subscriptionSummary));

    public void Configure(RemotingPushConsumerOptions options) => Configure(Options.DefaultName, options);

    public void Configure(string? name, RemotingPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!_consumerConfiguration.Owns(options))
        {
            return;
        }

        if (options.ConsumerMode != ConsumerMode.Clustering)
        {
            throw new InvalidOperationException("Remoting EventBus supports clustering-mode Push consumption only.");
        }

        if (options.ConsumeOrderly)
        {
            throw new InvalidOperationException("Remoting EventBus does not support orderly Push consumption.");
        }

        if (options.Subscriptions.Count != 0)
        {
            throw new InvalidOperationException(
                "Remoting EventBus owns Push consumer subscriptions. Do not call Subscribe in configureConsumer.");
        }

        options.ConsumeMessageBatchSize = 1;
        foreach (var subscription in _routePlan.Subscriptions)
        {
            options.Subscribe(subscription.Topic, new FilterExpression(subscription.FilterExpression));
        }

    }

    public ValidateOptionsResult Validate(string? name, RemotingPushConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!_consumerConfiguration.Owns(options))
        {
            return ValidateOptionsResult.Skip;
        }

        if (string.IsNullOrWhiteSpace(options.GroupName))
        {
            return ValidateOptionsResult.Fail("Consumer group name is required.");
        }

        if (options.ConsumerMode != ConsumerMode.Clustering)
        {
            return ValidateOptionsResult.Fail("Remoting EventBus requires clustering-mode Push consumption.");
        }

        if (options.ConsumeOrderly)
        {
            return ValidateOptionsResult.Fail("Remoting EventBus does not support orderly Push consumption.");
        }

        if (options.ConsumeMessageBatchSize != 1)
        {
            return ValidateOptionsResult.Fail("Remoting EventBus requires exactly one message per Push dispatch.");
        }

        if (!HasExpectedSubscriptions(options.Subscriptions))
        {
            return ValidateOptionsResult.Fail(
                "Remoting EventBus owns Push consumer subscriptions; they cannot be changed after EventBus configuration.");
        }

        _subscriptionSummary.Materialize(options.GroupName, _routePlan);
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
