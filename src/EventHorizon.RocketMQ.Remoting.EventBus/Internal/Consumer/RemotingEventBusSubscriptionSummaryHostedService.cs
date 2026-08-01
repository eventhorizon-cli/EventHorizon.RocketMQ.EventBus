namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Consumer;

internal sealed class RemotingEventBusSubscriptionSummaryHostedService(
    RemotingEventBusSubscriptionSummary subscriptionSummary,
    EventBusLoggingSettings loggingSettings,
    ILogger<RemotingEventBusSubscriptionSummaryHostedService> logger) : IHostedService
{
    private readonly RemotingEventBusSubscriptionSummary _subscriptionSummary =
        subscriptionSummary ?? throw new ArgumentNullException(nameof(subscriptionSummary));
    private readonly EventBusLoggingSettings _loggingSettings =
        loggingSettings ?? throw new ArgumentNullException(nameof(loggingSettings));
    private readonly ILogger<RemotingEventBusSubscriptionSummaryHostedService> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_loggingSettings.Enabled)
        {
            _subscriptionSummary.Write(_logger, _loggingSettings);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
