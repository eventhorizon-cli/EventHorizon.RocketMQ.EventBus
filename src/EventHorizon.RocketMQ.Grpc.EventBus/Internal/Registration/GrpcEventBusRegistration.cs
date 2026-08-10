using System.Reflection;
using System.Runtime.ExceptionServices;
using EventHorizon.RocketMQ.Grpc.EventBus.Internal.Consumer;
using EventHorizon.RocketMQ.Grpc.EventBus.Internal.Producer;

namespace EventHorizon.RocketMQ.Grpc.EventBus.Internal.Registration;

internal static class GrpcEventBusRegistration
{
    private static readonly MethodInfo AddPushConsumerMethod = typeof(GrpcEventBusRegistration)
        .GetMethod(nameof(AddPushConsumerCore), BindingFlags.NonPublic | BindingFlags.Static)!;

    internal static void AddPublisher(GrpcRocketMQBuilder builder, EventBusRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(registration);

        if (builder.RegistrationName is null)
        {
            builder.Services.AddSingleton<IEventBus>(serviceProvider =>
                new GrpcIntegrationEventBus(
                    registration,
                    serviceProvider.GetRequiredService<IGrpcProducer>(),
                    serviceProvider,
                    serviceProvider.GetRequiredService<ILogger<GrpcIntegrationEventBus>>()));
            return;
        }

        var registrationName = builder.RegistrationName;
        builder.Services.AddKeyedSingleton<IEventBus>(
            registrationName,
            (serviceProvider, _) => new GrpcIntegrationEventBus(
                registration,
                serviceProvider.GetRequiredKeyedService<IGrpcProducer>(registrationName),
                serviceProvider,
                serviceProvider.GetRequiredService<ILogger<GrpcIntegrationEventBus>>()));
    }

    internal static void AddPushConsumer(
        GrpcRocketMQBuilder builder,
        EventBusRegistration registration,
        GrpcEventBusConsumerOptions consumerOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(registration);

        try
        {
            AddPushConsumerMethod
                .MakeGenericMethod(registration.ConsumerAnchorHandlerType)
                .Invoke(null, [builder, registration, consumerOptions]);
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
            throw;
        }
    }

    private static void AddPushConsumerCore<TAnchorHandler>(
        GrpcRocketMQBuilder builder,
        EventBusRegistration registration,
        GrpcEventBusConsumerOptions consumerOptions)
        where TAnchorHandler : class
    {
        ArgumentNullException.ThrowIfNull(consumerOptions);
        var optionsMarker = new GrpcEventBusConsumerOptionsMarker();
        builder.AddGrpcPushConsumer<GrpcIntegrationEventBusHandler<TAnchorHandler>>(
            ServiceLifetime.Scoped,
            options =>
            {
                consumerOptions.ApplyTo(options);
                optionsMarker.Mark(options);
            });

        builder.Services.AddKeyedSingleton<GrpcEventBusConsumerConfiguration>(registration.Token);
        builder.Services.AddSingleton<IConfigureOptions<GrpcPushConsumerOptions>>(serviceProvider =>
            CreateConsumerOptionsSetup(serviceProvider, registration, optionsMarker));
        builder.Services.AddSingleton<IValidateOptions<GrpcPushConsumerOptions>>(serviceProvider =>
            CreateConsumerOptionsSetup(serviceProvider, registration, optionsMarker));

        builder.Services.AddSingleton<IHostedService>(serviceProvider =>
            new GrpcEventBusSubscriptionSummaryHostedService(
                registration,
                serviceProvider.GetRequiredKeyedService<GrpcEventBusConsumerConfiguration>(registration.Token),
                registration.GetRequiredLoggingSettings(serviceProvider),
                serviceProvider.GetRequiredService<ILogger<GrpcEventBusSubscriptionSummaryHostedService>>()));
    }

    private static GrpcEventBusConsumerOptionsSetup CreateConsumerOptionsSetup(
        IServiceProvider serviceProvider,
        EventBusRegistration registration,
        GrpcEventBusConsumerOptionsMarker optionsMarker) =>
        new(
            optionsMarker,
            registration.GetRequiredRoutePlan(serviceProvider),
            serviceProvider.GetRequiredKeyedService<GrpcEventBusConsumerConfiguration>(registration.Token));
}
