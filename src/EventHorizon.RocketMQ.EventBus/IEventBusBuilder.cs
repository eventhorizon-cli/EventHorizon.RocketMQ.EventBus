using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.EventBus;

/// <summary>
/// Configures one EventBus registration before its service provider is built.
/// </summary>
public interface IEventBusBuilder
{
    /// <summary>
    /// Gets the service collection to which the EventBus registration is being added.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Gets the registration name, or <see langword="null"/> for the default registration.
    /// </summary>
    string? RegistrationName { get; }
}
