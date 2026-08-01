using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.EventBus.Internal.Registration;

internal sealed class EventBusBuilder : IEventBusBuilder
{
    internal EventBusBuilder(EventBusRegistration registration)
    {
        Registration = registration;
    }

    public IServiceCollection Services => Registration.Services;

    public string? RegistrationName => Registration.RegistrationName;

    internal EventBusRegistration Registration { get; }
}
