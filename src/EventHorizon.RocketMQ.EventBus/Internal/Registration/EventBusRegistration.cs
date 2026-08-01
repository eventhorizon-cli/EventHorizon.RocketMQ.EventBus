using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.EventBus.Internal.Registration;

/// <summary>
/// Transport-neutral configuration state shared with the friend adapter assemblies.
/// </summary>
/// <remarks>
/// This type is not registered as a runtime service. Runtime services are keyed by <see cref="Token"/> and use the
/// provider's descriptor snapshot, so later service-collection changes cannot affect an already built provider.
/// </remarks>
internal sealed class EventBusRegistration
{
    private static readonly MethodInfo AddRegistrationAccessorMethod = typeof(EventBusRegistration)
        .GetMethod(nameof(AddRegistrationAccessor), BindingFlags.NonPublic | BindingFlags.Static)!;

    private readonly Action<EventBusRegistration>? _ensureConsumer;
    private EventBusLoggingSettings _loggingSettings = new(Enabled: true, IncludePayload: true);
    private bool _consumerAdded;
    private Type? _consumerAnchorHandlerType;

    private EventBusRegistration(
        IServiceCollection services,
        string? registrationName,
        object token,
        Action<EventBusRegistration>? ensureConsumer)
    {
        Services = services;
        RegistrationName = registrationName;
        Token = token;
        _ensureConsumer = ensureConsumer;
        Builder = new EventBusBuilder(this);
    }

    internal IServiceCollection Services { get; }

    internal string? RegistrationName { get; }

    internal object Token { get; }

    internal IEventBusBuilder Builder { get; }

    internal Type ConsumerAnchorHandlerType => _consumerAnchorHandlerType ??
        throw new InvalidOperationException("The EventBus registration does not contain a Handler.");

    internal static EventBusRegistration Create(
        IServiceCollection services,
        string? registrationName,
        Action<EventBusRegistration>? ensureConsumer = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(EventBusRegistrationMarker) &&
                descriptor.ImplementationInstance is EventBusRegistrationMarker existing &&
                string.Equals(existing.RegistrationName, registrationName, StringComparison.Ordinal))
            {
                var displayName = registrationName ?? "<default>";
                throw new InvalidOperationException($"An EventBus registration named '{displayName}' already exists.");
            }
        }

        var token = new object();
        var registration = new EventBusRegistration(services, registrationName, token, ensureConsumer);
        services.AddSingleton(new EventBusRegistrationMarker(registrationName, token));
        services.AddKeyedSingleton(token, registration._loggingSettings);
        services.AddKeyedSingleton<IIntegrationEventSerializer, NewtonsoftJsonIntegrationEventSerializer>(token);
        services.AddKeyedSingleton<IEventBusRoutePlan>(
            token,
            (serviceProvider, _) => EventBusRoutePlan.Create(serviceProvider.GetKeyedServices<EventBusHandlerRegistration>(token)));
        services.AddKeyedScoped<IEventBusDispatchRuntime>(
            token,
            (serviceProvider, _) => new EventBusDispatchRuntime(
                token,
                serviceProvider.GetRequiredKeyedService<IEventBusRoutePlan>(token),
                serviceProvider.GetRequiredKeyedService<IIntegrationEventSerializer>(token),
                serviceProvider));

        return registration;
    }

    internal void AddHandler(Type handlerType, ServiceLifetime handlerLifetime)
    {
        ValidateLifetime(handlerLifetime);
        var eventTypes = GetHandledEventTypes(handlerType);
        ValidateHandlerOwnership(handlerType);
        var existing = GetHandlerRegistrations();

        if (existing.Any(registration => registration.HandlerType == handlerType && registration.Lifetime != handlerLifetime))
        {
            throw new InvalidOperationException(
                $"Handler type '{handlerType.FullName}' cannot use more than one lifetime in the same EventBus registration.");
        }

        var additions = new List<EventBusHandlerRegistration>();
        foreach (var eventType in eventTypes)
        {
            var route = EventBusRouteDefinition.Create(eventType);
            ValidateRoute(route, existing);
            ValidateRoute(route, additions);

            if (existing.Any(registration =>
                    registration.HandlerType == handlerType && registration.IntegrationEventType == eventType))
            {
                continue;
            }

            additions.Add(EventBusHandlerRegistration.Create(handlerType, route, handlerLifetime));
        }

        if (additions.Count > 0)
        {
            if (!existing.Any(registration => registration.HandlerType == handlerType))
            {
                Services.AddSingleton(new EventBusHandlerOwnership(handlerType, RegistrationName, Token));
                _consumerAnchorHandlerType ??= handlerType;
                AddRegistrationAccessorMethod.MakeGenericMethod(handlerType).Invoke(null, [Services, this]);
                Services.Add(ServiceDescriptor.DescribeKeyed(handlerType, Token, handlerType, handlerLifetime));
            }

            foreach (var addition in additions)
            {
                Services.AddKeyedSingleton<EventBusHandlerRegistration>(Token, addition);
            }
        }

        EnsureConsumer();
    }

    internal void AddHandlersFromAssembly(Assembly assembly, ServiceLifetime handlerLifetime)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        ValidateLifetime(handlerLifetime);

        Type[] types;
        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException exception)
        {
            throw new InvalidOperationException($"Unable to inspect assembly '{assembly.FullName}' for EventBus handlers.", exception);
        }

        foreach (var handlerType in types
                     .Where(IsConcreteHandlerCandidate)
                     .Where(static type => GetHandledEventTypes(type, throwWhenInvalid: false).Count > 0)
                     .OrderBy(static type => type.FullName ?? type.Name, StringComparer.Ordinal))
        {
            AddHandler(handlerType, handlerLifetime);
        }
    }

    internal void UseSerializer(Type serializerType)
    {
        ArgumentNullException.ThrowIfNull(serializerType);

        if (!serializerType.IsClass || serializerType.IsAbstract || serializerType.ContainsGenericParameters ||
            !typeof(IIntegrationEventSerializer).IsAssignableFrom(serializerType))
        {
            throw new ArgumentException(
                $"Serializer type '{serializerType.FullName}' must be a concrete {nameof(IIntegrationEventSerializer)} implementation.",
                nameof(serializerType));
        }

        for (var index = Services.Count - 1; index >= 0; index--)
        {
            var descriptor = Services[index];
            if (descriptor.ServiceType == typeof(IIntegrationEventSerializer) && ReferenceEquals(descriptor.ServiceKey, Token))
            {
                Services.RemoveAt(index);
            }
        }

        Services.Add(ServiceDescriptor.DescribeKeyed(
            typeof(IIntegrationEventSerializer),
            Token,
            serializerType,
            ServiceLifetime.Singleton));
    }

    internal void ConfigureLogging(Action<EventBusLoggingOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new EventBusLoggingOptions
        {
            Enabled = _loggingSettings.Enabled,
            IncludePayload = _loggingSettings.IncludePayload,
        };
        configure(options);
        _loggingSettings = new EventBusLoggingSettings(options.Enabled, options.IncludePayload);

        for (var index = Services.Count - 1; index >= 0; index--)
        {
            var descriptor = Services[index];
            if (descriptor.ServiceType == typeof(EventBusLoggingSettings) && ReferenceEquals(descriptor.ServiceKey, Token))
            {
                Services.RemoveAt(index);
            }
        }

        Services.AddKeyedSingleton(Token, _loggingSettings);
    }

    /// <summary>
    /// Resolves the serializer isolated to this EventBus registration.
    /// </summary>
    internal IIntegrationEventSerializer GetRequiredSerializer(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        return serviceProvider.GetRequiredKeyedService<IIntegrationEventSerializer>(Token);
    }

    internal EventBusLoggingSettings GetRequiredLoggingSettings(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        return serviceProvider.GetRequiredKeyedService<EventBusLoggingSettings>(Token);
    }

    /// <summary>
    /// Resolves the immutable route plan isolated to this EventBus registration.
    /// </summary>
    internal IEventBusRoutePlan GetRequiredRoutePlan(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        return serviceProvider.GetRequiredKeyedService<IEventBusRoutePlan>(Token);
    }

    /// <summary>
    /// Resolves the scoped dispatcher for this registration's existing transport delivery scope.
    /// </summary>
    internal IEventBusDispatchRuntime GetRequiredDispatcher(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        return serviceProvider.GetRequiredKeyedService<IEventBusDispatchRuntime>(Token);
    }

    /// <summary>
    /// Creates a publish exception without including a message body in its metadata.
    /// </summary>
    internal EventBusPublishException CreatePublishException(
        IntegrationEvent integrationEvent,
        string? transportResult = null,
        Exception? innerException = null)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);
        return new EventBusPublishException(
            integrationEvent.GetType(),
            integrationEvent.Topic,
            integrationEvent.Tag,
            RegistrationName,
            transportResult,
            innerException);
    }

    private void EnsureConsumer()
    {
        if (_consumerAdded)
        {
            return;
        }

        _ensureConsumer?.Invoke(this);
        _consumerAdded = true;
    }

    private IReadOnlyList<EventBusHandlerRegistration> GetHandlerRegistrations() => Services
        .Where(descriptor =>
            descriptor.ServiceType == typeof(EventBusHandlerRegistration) && ReferenceEquals(descriptor.ServiceKey, Token))
        .Select(static descriptor => descriptor.KeyedImplementationInstance)
        .OfType<EventBusHandlerRegistration>()
        .ToArray();

    private void ValidateHandlerOwnership(Type handlerType)
    {
        var existing = Services
            .Where(static descriptor => descriptor.ServiceType == typeof(EventBusHandlerOwnership))
            .Select(static descriptor => descriptor.ImplementationInstance)
            .OfType<EventBusHandlerOwnership>()
            .FirstOrDefault(ownership => ownership.HandlerType == handlerType);
        if (existing is null || ReferenceEquals(existing.RegistrationToken, Token))
        {
            return;
        }

        var handlerName = handlerType.FullName ?? handlerType.Name;
        var existingRegistration = existing.RegistrationName ?? "<default>";
        var requestedRegistration = RegistrationName ?? "<default>";
        throw new InvalidOperationException(
            $"Handler type '{handlerName}' is already registered with EventBus registration " +
            $"'{existingRegistration}' and cannot also be registered with '{requestedRegistration}'.");
    }

    private static IReadOnlyList<Type> GetHandledEventTypes(Type handlerType, bool throwWhenInvalid = true)
    {
        ArgumentNullException.ThrowIfNull(handlerType);

        if (!IsConcreteHandlerCandidate(handlerType))
        {
            if (throwWhenInvalid)
            {
                throw new ArgumentException(
                    $"Handler type '{handlerType.FullName}' must be a concrete, closed class.",
                    nameof(handlerType));
            }

            return [];
        }

        var eventTypes = handlerType.GetInterfaces()
            .Where(static candidate =>
                candidate.IsGenericType &&
                candidate.GetGenericTypeDefinition() == typeof(IIntegrationEventBusHandler<>) &&
                !candidate.ContainsGenericParameters)
            .Select(static candidate => candidate.GenericTypeArguments[0])
            .OrderBy(static type => type.FullName ?? type.Name, StringComparer.Ordinal)
            .ToArray();

        if (eventTypes.Length == 0 && throwWhenInvalid)
        {
            throw new ArgumentException(
                $"Handler type '{handlerType.FullName}' does not implement a closed {nameof(IIntegrationEventBusHandler<IntegrationEvent>)} interface.",
                nameof(handlerType));
        }

        foreach (var eventType in eventTypes)
        {
            EventBusRouteDefinition.ValidateIntegrationEventType(eventType);
        }

        return eventTypes;
    }

    private static bool IsConcreteHandlerCandidate(Type type) =>
        type.IsClass && !type.IsAbstract && !type.ContainsGenericParameters;

    private static void AddRegistrationAccessor<TAnchorHandler>(
        IServiceCollection services,
        EventBusRegistration registration)
        where TAnchorHandler : class =>
        services.AddScoped(serviceProvider =>
            new EventBusRegistrationAccessor<TAnchorHandler>(registration, serviceProvider));

    private static void ValidateRoute(
        EventBusRouteDefinition route,
        IEnumerable<EventBusHandlerRegistration> registrations)
    {
        foreach (var registration in registrations)
        {
            if (registration.Route.Key.Equals(route.Key) && registration.IntegrationEventType != route.IntegrationEventType)
            {
                throw new InvalidOperationException(
                    $"Route '{route.Topic}' and '{route.Tag}' is already assigned to integration event type " +
                    $"'{registration.IntegrationEventType.FullName}'.");
            }

            if (registration.IntegrationEventType == route.IntegrationEventType && !registration.Route.Key.Equals(route.Key))
            {
                throw new InvalidOperationException(
                    $"Integration event type '{route.IntegrationEventType.FullName}' produced more than one route during registration.");
            }
        }
    }

    private static void ValidateLifetime(ServiceLifetime lifetime)
    {
        if (lifetime is not (ServiceLifetime.Singleton or ServiceLifetime.Scoped or ServiceLifetime.Transient))
        {
            throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, "Unsupported handler lifetime.");
        }
    }

    private sealed class EventBusRegistrationMarker(string? registrationName, object token)
    {
        internal string? RegistrationName { get; } = registrationName;

        internal object Token { get; } = token;
    }

    private sealed class EventBusHandlerOwnership(Type handlerType, string? registrationName, object registrationToken)
    {
        internal Type HandlerType { get; } = handlerType;

        internal string? RegistrationName { get; } = registrationName;

        internal object RegistrationToken { get; } = registrationToken;
    }
}
