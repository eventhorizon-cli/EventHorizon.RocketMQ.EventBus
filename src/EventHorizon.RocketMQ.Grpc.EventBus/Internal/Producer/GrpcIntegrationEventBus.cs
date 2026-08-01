using System.Diagnostics;

namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Producer;

internal sealed class GrpcIntegrationEventBus(
    EventBusRegistration registration,
    IGrpcProducer producer,
    IServiceProvider serviceProvider,
    ILogger<GrpcIntegrationEventBus> logger) : IEventBus
{
    private readonly EventBusRegistration _registration = registration ?? throw new ArgumentNullException(nameof(registration));
    private readonly IGrpcProducer _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly ILogger<GrpcIntegrationEventBus> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    public async Task PublishAsync(IntegrationEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);

        var startedAt = Stopwatch.GetTimestamp();
        IIntegrationEventSerializer? serializer = null;
        byte[]? body = null;
        try
        {
            serializer = _registration.GetRequiredSerializer(_serviceProvider);
            body = serializer.Serialize(integrationEvent);
            var message = new Message(integrationEvent.Topic, body)
            {
                Tag = integrationEvent.Tag,
            };
            var receipt = await _producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
            var loggingSettings = _registration.GetRequiredLoggingSettings(_serviceProvider);
            _logger.LogEventBusPublishSucceeded(
                loggingSettings,
                integrationEvent,
                receipt.MessageId,
                Stopwatch.GetElapsedTime(startedAt),
                serializer,
                body);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            var publishException = _registration.CreatePublishException(integrationEvent, innerException: exception);
            var loggingSettings = _registration.GetRequiredLoggingSettings(_serviceProvider);
            _logger.LogEventBusPublishFailed(
                loggingSettings,
                publishException,
                integrationEvent,
                Stopwatch.GetElapsedTime(startedAt),
                serializer,
                body);
            throw publishException;
        }
    }
}
