namespace EventHorizon.RocketMQ.EventBus.Internal.Registration;

/// <summary>
/// Resolves one EventBus registration from the transport's existing delivery scope.
/// </summary>
/// <typeparam name="TAnchorHandler">
/// The first application Handler owned by the registration. Its type makes the accessor unique without exposing a
/// transport-owned registration key.
/// </typeparam>
internal sealed class EventBusRegistrationAccessor<TAnchorHandler>
    where TAnchorHandler : class
{
    private readonly EventBusRegistration _registration;
    private readonly IServiceProvider _serviceProvider;

    internal EventBusRegistrationAccessor(
        EventBusRegistration registration,
        IServiceProvider serviceProvider)
    {
        _registration = registration ?? throw new ArgumentNullException(nameof(registration));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    internal string? RegistrationName => _registration.RegistrationName;

    internal IEventBusRoutePlan RoutePlan => _registration.GetRequiredRoutePlan(_serviceProvider);

    internal IIntegrationEventSerializer Serializer => _registration.GetRequiredSerializer(_serviceProvider);

    internal EventBusLoggingSettings LoggingSettings => _registration.GetRequiredLoggingSettings(_serviceProvider);

    internal IEventBusDispatchRuntime Dispatcher => _registration.GetRequiredDispatcher(_serviceProvider);

    internal EventBusPublishException CreatePublishException(
        IntegrationEvent integrationEvent,
        string? transportResult = null,
        Exception? innerException = null) =>
        _registration.CreatePublishException(integrationEvent, transportResult, innerException);
}
