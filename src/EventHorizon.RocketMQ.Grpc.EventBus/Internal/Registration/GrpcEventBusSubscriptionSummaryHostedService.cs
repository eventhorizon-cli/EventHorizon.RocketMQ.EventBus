namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Registration;

internal sealed class GrpcEventBusSubscriptionSummaryHostedService(
    EventBusRegistration registration,
    GrpcEventBusConsumerConfiguration consumerConfiguration,
    EventBusLoggingSettings loggingSettings,
    ILogger<GrpcEventBusSubscriptionSummaryHostedService> logger) : IHostedService
{
    private readonly EventBusRegistration _registration = registration ?? throw new ArgumentNullException(nameof(registration));
    private readonly GrpcEventBusConsumerConfiguration _consumerConfiguration =
        consumerConfiguration ?? throw new ArgumentNullException(nameof(consumerConfiguration));
    private readonly EventBusLoggingSettings _loggingSettings =
        loggingSettings ?? throw new ArgumentNullException(nameof(loggingSettings));
    private readonly ILogger<GrpcEventBusSubscriptionSummaryHostedService> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));
    private int _written;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_loggingSettings.Enabled)
        {
            return Task.CompletedTask;
        }

        var snapshot = _consumerConfiguration.GetRequiredSnapshot();
        if (Interlocked.CompareExchange(ref _written, 1, 0) != 0)
        {
            return Task.CompletedTask;
        }

        _logger.LogEventBusSubscriptionsMaterialized(
            _loggingSettings,
            _registration.RegistrationName ?? "<default>",
            snapshot.GroupName,
            snapshot.HandlerCount,
            snapshot.Subscriptions);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
