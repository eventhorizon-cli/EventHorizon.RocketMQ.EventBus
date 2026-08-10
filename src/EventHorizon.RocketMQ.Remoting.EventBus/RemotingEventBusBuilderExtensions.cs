using System.Reflection;
using System.Runtime.ExceptionServices;
using EventHorizon.RocketMQ.Remoting.EventBus.Internal.Consumer;
using EventHorizon.RocketMQ.Remoting.EventBus.Internal.Producer;

namespace EventHorizon.RocketMQ.Remoting.EventBus;

/// <summary>
/// Adds strongly typed EventBus roles to a classic Remoting RocketMQ client registration.
/// </summary>
public static class RemotingEventBusBuilderExtensions
{
    private static readonly MethodInfo AddConsumerMethod = typeof(RemotingEventBusBuilderExtensions)
        .GetMethod(nameof(AddConsumer), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>
    /// Adds an EventBus registration to the supplied classic Remoting RocketMQ client.
    /// </summary>
    /// <param name="builder">The Remoting client registration to extend.</param>
    /// <param name="configureConsumer">
    /// An optional EventBus Push consumer configuration delegate. It takes effect after the first EventBus Handler is
    /// registered.
    /// </param>
    /// <param name="configureProducer">
    /// An optional Producer configuration delegate. A non-<see langword="null"/> value enables publishing and
    /// registers <see cref="IEventBus"/> for this EventBus registration.
    /// </param>
    /// <returns>A builder used to register Handlers and configure serialization.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// An EventBus already uses the same default or named registration identity.
    /// </exception>
    public static IEventBusBuilder AddRemotingEventBus(
        this RemotingRocketMQBuilder builder,
        Action<RemotingEventBusConsumerOptions>? configureConsumer = null,
        Action<RemotingEventBusProducerOptions>? configureProducer = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var consumerOptions = new RemotingEventBusConsumerOptions();
        configureConsumer?.Invoke(consumerOptions);
        consumerOptions = consumerOptions.Snapshot();

        var registration = EventBusRegistration.Create(
            builder.Services,
            builder.RegistrationName,
            eventBusRegistration => AddConsumerWithAnchor(builder, eventBusRegistration, consumerOptions),
            skipDeserializationFailures: consumerOptions.SkipDeserializationFailures);

        if (configureProducer is not null)
        {
            var producerOptions = new RemotingEventBusProducerOptions();
            configureProducer(producerOptions);
            producerOptions = producerOptions.Snapshot();
            builder.AddRemotingProducer(producerOptions.ApplyTo);
            AddPublisher(builder, registration);
        }

        return registration.Builder;
    }

    private static void AddConsumerWithAnchor(
        RemotingRocketMQBuilder builder,
        EventBusRegistration registration,
        RemotingEventBusConsumerOptions consumerOptions)
    {
        try
        {
            // This closes the bridge at startup; delivery dispatch never reflects over application Handlers.
            AddConsumerMethod.MakeGenericMethod(registration.ConsumerAnchorHandlerType).Invoke(
                null,
                [builder, registration, consumerOptions]);
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
            throw;
        }
    }

    private static void AddConsumer<TAnchorHandler>(
        RemotingRocketMQBuilder builder,
        EventBusRegistration registration,
        RemotingEventBusConsumerOptions consumerOptions)
        where TAnchorHandler : class
    {
        var consumerConfiguration = new RemotingEventBusConsumerConfiguration(consumerOptions);

        builder.AddRemotingPushConsumer<RemotingEventBusPushMessageHandler<TAnchorHandler>>(
            ServiceLifetime.Scoped,
            consumerConfiguration.Configure);
        builder.Services.AddKeyedSingleton(
            registration.Token,
            (IServiceProvider _, object? _) => new RemotingEventBusSubscriptionSummary(registration));
        builder.Services.AddSingleton<IConfigureOptions<RemotingPushConsumerOptions>>(serviceProvider =>
            CreateConsumerOptionsSetup(serviceProvider, registration, consumerConfiguration));
        builder.Services.AddSingleton<IValidateOptions<RemotingPushConsumerOptions>>(serviceProvider =>
            CreateConsumerOptionsSetup(serviceProvider, registration, consumerConfiguration));
        builder.Services.AddSingleton<IHostedService>(serviceProvider =>
            new RemotingEventBusSubscriptionSummaryHostedService(
                serviceProvider.GetRequiredKeyedService<RemotingEventBusSubscriptionSummary>(registration.Token),
                registration.GetRequiredLoggingSettings(serviceProvider),
                serviceProvider.GetRequiredService<ILogger<RemotingEventBusSubscriptionSummaryHostedService>>()));
    }

    private static RemotingEventBusConsumerOptionsSetup CreateConsumerOptionsSetup(
        IServiceProvider serviceProvider,
        EventBusRegistration registration,
        RemotingEventBusConsumerConfiguration consumerConfiguration) =>
        new(
            consumerConfiguration,
            registration.GetRequiredRoutePlan(serviceProvider),
            serviceProvider.GetRequiredKeyedService<RemotingEventBusSubscriptionSummary>(registration.Token));

    private static void AddPublisher(RemotingRocketMQBuilder builder, EventBusRegistration registration)
    {
        if (builder.RegistrationName is null)
        {
            builder.Services.AddSingleton<IEventBus>(serviceProvider => CreatePublisher(serviceProvider, registration, null));
            return;
        }

        var registrationName = builder.RegistrationName;
        builder.Services.AddKeyedSingleton<IEventBus>(
            registrationName,
            (serviceProvider, _) => CreatePublisher(serviceProvider, registration, registrationName));
    }

    private static IEventBus CreatePublisher(
        IServiceProvider serviceProvider,
        EventBusRegistration registration,
        string? registrationName)
    {
        var producer = registrationName is null
            ? serviceProvider.GetRequiredService<IRemotingProducer>()
            : serviceProvider.GetRequiredKeyedService<IRemotingProducer>(registrationName);
        var logger = serviceProvider.GetRequiredService<ILogger<RemotingEventBus>>();
        return new RemotingEventBus(registration, serviceProvider, producer, logger);
    }
}
