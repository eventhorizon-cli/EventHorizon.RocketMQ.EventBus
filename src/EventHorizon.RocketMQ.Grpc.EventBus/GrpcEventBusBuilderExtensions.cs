namespace EventHorizon.RocketMQ.Grpc.EventBus;

/// <summary>
/// Provides registration methods for the RocketMQ gRPC EventBus adapter.
/// </summary>
public static class GrpcEventBusBuilderExtensions
{
    /// <summary>
    /// Adds the strongly typed EventBus adapter to a RocketMQ gRPC client registration.
    /// </summary>
    /// <param name="builder">The RocketMQ gRPC client builder to extend.</param>
    /// <param name="configureConsumer">
    /// An optional delegate that configures the Push consumer except its EventBus-owned subscriptions.
    /// </param>
    /// <param name="configureProducer">
    /// An optional delegate that enables and configures EventBus publishing through a gRPC Producer.
    /// </param>
    /// <returns>A builder used to register handlers and replace the serializer.</returns>
    /// <remarks>
    /// Supplying <paramref name="configureProducer"/> creates a Producer and exposes <see cref="IEventBus"/> for
    /// this registration. Registering the first application Handler creates a scoped Push consumer bridge. The
    /// EventBus owns all Push consumer subscriptions, so <paramref name="configureConsumer"/> must not call
    /// <c>Subscribe</c>.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// An EventBus registration with the same default or named identity already exists, or a conflicting main-client
    /// role has already been registered.
    /// </exception>
    public static IEventBusBuilder AddGrpcEventBus(
        this GrpcRocketMQBuilder builder,
        Action<GrpcPushConsumerOptions>? configureConsumer = null,
        Action<GrpcProducerOptions>? configureProducer = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var registration = EventBusRegistration.Create(
            builder.Services,
            builder.RegistrationName,
            eventBusRegistration => GrpcEventBusRegistration.AddPushConsumer(
                builder,
                eventBusRegistration,
                configureConsumer));

        if (configureProducer is not null)
        {
            builder.AddGrpcProducer(configureProducer);
            GrpcEventBusRegistration.AddPublisher(builder, registration);
        }

        return registration.Builder;
    }
}
