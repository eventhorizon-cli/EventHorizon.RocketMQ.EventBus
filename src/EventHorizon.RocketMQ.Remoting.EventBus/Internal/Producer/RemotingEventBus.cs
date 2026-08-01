using System.Diagnostics;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Internal.Producer;

internal sealed class RemotingEventBus(
    EventBusRegistration registration,
    IServiceProvider serviceProvider,
    IRemotingProducer producer,
    ILogger<RemotingEventBus> logger) : IEventBus
{
    private readonly EventBusRegistration _registration = registration ?? throw new ArgumentNullException(nameof(registration));
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IRemotingProducer _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    private readonly ILogger<RemotingEventBus> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    public async Task PublishAsync(IntegrationEvent integrationEvent, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);

        var startedAt = Stopwatch.GetTimestamp();
        IIntegrationEventSerializer? serializer = null;
        byte[] payload;
        try
        {
            serializer = _registration.GetRequiredSerializer(_serviceProvider);
            payload = serializer.Serialize(integrationEvent);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            var publishException = _registration.CreatePublishException(integrationEvent, innerException: exception);
            LogPublishFailure(
                publishException,
                integrationEvent,
                serializer,
                messageId: null,
                Stopwatch.GetElapsedTime(startedAt),
                payload: null);
            throw publishException;
        }

        try
        {
            var message = new Message(integrationEvent.Topic, payload) { Tag = integrationEvent.Tag };
            var result = await _producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
            if (result.Status != RemotingSendStatus.SendOk)
            {
                var publishException = _registration.CreatePublishException(
                    integrationEvent,
                    transportResult: result.Status.ToString());
                LogPublishFailure(
                    publishException,
                    integrationEvent,
                    serializer,
                    result.MessageId,
                    Stopwatch.GetElapsedTime(startedAt),
                    payload);
                throw publishException;
            }

            _logger.LogEventBusPublishSucceeded(
                _registration.GetRequiredLoggingSettings(_serviceProvider),
                integrationEvent,
                result.MessageId,
                Stopwatch.GetElapsedTime(startedAt),
                serializer,
                payload);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (EventBusPublishException)
        {
            throw;
        }
        catch (Exception exception)
        {
            var publishException = _registration.CreatePublishException(integrationEvent, innerException: exception);
            LogPublishFailure(
                publishException,
                integrationEvent,
                serializer,
                messageId: null,
                Stopwatch.GetElapsedTime(startedAt),
                payload);
            throw publishException;
        }
    }

    private void LogPublishFailure(
        EventBusPublishException exception,
        IntegrationEvent integrationEvent,
        IIntegrationEventSerializer? serializer,
        string? messageId,
        TimeSpan duration,
        ReadOnlyMemory<byte>? payload) =>
        _logger.LogEventBusPublishFailed(
            _registration.GetRequiredLoggingSettings(_serviceProvider),
            exception,
            integrationEvent,
            messageId,
            duration,
            serializer,
            payload);
}
