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
    /// An optional delegate that configures the EventBus-owned Push consumer.
    /// </param>
    /// <param name="configureProducer">
    /// An optional delegate that enables and configures EventBus publishing through a gRPC Producer.
    /// </param>
    /// <returns>A builder used to register handlers and replace the serializer.</returns>
    /// <remarks>
    /// Supplying <paramref name="configureProducer"/> creates a Producer and exposes <see cref="IEventBus"/> for
    /// this registration. Registering the first application Handler creates a scoped Push consumer bridge. The
    /// EventBus owns all Push consumer subscriptions. Its option wrapper exposes only supported EventBus settings.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// An EventBus registration with the same default or named identity already exists, or a conflicting main-client
    /// role has already been registered.
    /// </exception>
    public static IEventBusBuilder AddGrpcEventBus(
        this GrpcRocketMQBuilder builder,
        Action<GrpcEventBusConsumerOptions>? configureConsumer = null,
        Action<GrpcEventBusProducerOptions>? configureProducer = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var consumerOptions = new GrpcEventBusConsumerOptions();
        configureConsumer?.Invoke(consumerOptions);
        consumerOptions = consumerOptions.Snapshot();

        var registration = EventBusRegistration.Create(
            builder.Services,
            builder.RegistrationName,
            eventBusRegistration => GrpcEventBusRegistration.AddPushConsumer(
                builder,
                eventBusRegistration,
                consumerOptions),
            skipDeserializationFailures: consumerOptions.SkipDeserializationFailures);

        if (configureProducer is not null)
        {
            var producerOptions = new GrpcEventBusProducerOptions();
            configureProducer(producerOptions);
            producerOptions = producerOptions.Snapshot();
            builder.AddGrpcProducer(producerOptions.ApplyTo);
            GrpcEventBusRegistration.AddPublisher(builder, registration);
        }

        return registration.Builder;
    }
}
