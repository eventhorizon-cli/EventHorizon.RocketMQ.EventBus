using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.EventBus;

/// <summary>
/// Provides startup-time registration methods for EventBus handlers and serializers.
/// </summary>
public static class EventBusBuilderExtensions
{
    /// <summary>
    /// Configures EventBus logging for this registration.
    /// </summary>
    /// <param name="builder">The EventBus registration builder.</param>
    /// <param name="configure">The logging configuration delegate.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    public static IEventBusBuilder ConfigureLogging(
        this IEventBusBuilder builder,
        Action<EventBusLoggingOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        GetRegistration(builder).ConfigureLogging(configure);
        return builder;
    }

    /// <summary>
    /// Registers a handler and every closed integration-event handler interface it implements.
    /// </summary>
    /// <typeparam name="THandler">The concrete handler type.</typeparam>
    /// <param name="builder">The EventBus registration builder.</param>
    /// <param name="handlerLifetime">The dependency-injection lifetime for the handler.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><typeparamref name="THandler"/> is not a valid integration-event handler.</exception>
    /// <exception cref="InvalidOperationException">
    /// The registration would create an ambiguous route or lifetime conflict, or the Handler belongs to another
    /// EventBus registration.
    /// </exception>
    public static IEventBusBuilder AddHandler<THandler>(
        this IEventBusBuilder builder,
        ServiceLifetime handlerLifetime = ServiceLifetime.Scoped)
        where THandler : class
    {
        GetRegistration(builder).AddHandler(typeof(THandler), handlerLifetime);
        return builder;
    }

    /// <summary>
    /// Discovers integration-event handlers from the assembly containing a marker type.
    /// </summary>
    /// <typeparam name="TMarker">A type in the assembly to scan.</typeparam>
    /// <param name="builder">The EventBus registration builder.</param>
    /// <param name="handlerLifetime">The dependency-injection lifetime for discovered handlers.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// A discovered Handler creates an invalid route or lifetime conflict, or belongs to another EventBus
    /// registration.
    /// </exception>
    public static IEventBusBuilder AddHandlersFromAssemblyOf<TMarker>(
        this IEventBusBuilder builder,
        ServiceLifetime handlerLifetime = ServiceLifetime.Scoped)
    {
        GetRegistration(builder).AddHandlersFromAssembly(typeof(TMarker).Assembly, handlerLifetime);
        return builder;
    }

    /// <summary>
    /// Discovers integration-event handlers from an assembly.
    /// </summary>
    /// <param name="builder">The EventBus registration builder.</param>
    /// <param name="assembly">The assembly to scan.</param>
    /// <param name="handlerLifetime">The dependency-injection lifetime for discovered handlers.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="assembly"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// A discovered Handler creates an invalid route or lifetime conflict, or belongs to another EventBus
    /// registration.
    /// </exception>
    public static IEventBusBuilder AddHandlersFromAssembly(
        this IEventBusBuilder builder,
        Assembly assembly,
        ServiceLifetime handlerLifetime = ServiceLifetime.Scoped)
    {
        GetRegistration(builder).AddHandlersFromAssembly(assembly, handlerLifetime);
        return builder;
    }

    /// <summary>
    /// Replaces the default serializer for this EventBus registration.
    /// </summary>
    /// <typeparam name="TSerializer">The thread-safe serializer implementation.</typeparam>
    /// <param name="builder">The EventBus registration builder.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><typeparamref name="TSerializer"/> is not a concrete serializer implementation.</exception>
    public static IEventBusBuilder UseSerializer<TSerializer>(this IEventBusBuilder builder)
        where TSerializer : class, IIntegrationEventSerializer
    {
        GetRegistration(builder).UseSerializer(typeof(TSerializer));
        return builder;
    }

    private static EventBusRegistration GetRegistration(IEventBusBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder is EventBusBuilder eventBusBuilder
            ? eventBusBuilder.Registration
            : throw new ArgumentException(
                "The EventBus builder must originate from a transport EventBus registration.",
                nameof(builder));
    }
}
