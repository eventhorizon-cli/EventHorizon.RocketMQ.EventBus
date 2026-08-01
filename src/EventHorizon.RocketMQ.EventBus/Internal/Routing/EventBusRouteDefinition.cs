using System.Reflection;

namespace EventHorizon.RocketMQ.EventBus.Internal.Routing;

internal sealed class EventBusRouteDefinition
{
    private EventBusRouteDefinition(Type integrationEventType, string topic, string? tag)
    {
        IntegrationEventType = integrationEventType;
        Topic = topic;
        Tag = tag;
        Key = new EventBusRouteKey(topic, tag);
    }

    internal Type IntegrationEventType { get; }

    internal string Topic { get; }

    internal string? Tag { get; }

    internal EventBusRouteKey Key { get; }

    internal static EventBusRouteDefinition Create(Type integrationEventType)
    {
        ValidateIntegrationEventType(integrationEventType);

        var constructor = integrationEventType.GetConstructor(
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            Type.EmptyTypes,
            modifiers: null);
        if (constructor is null)
        {
            throw new InvalidOperationException(
                $"Integration event type '{integrationEventType.FullName}' must have a public parameterless constructor.");
        }

        var integrationEvent = ConstructIntegrationEvent(integrationEventType, constructor);
        var verificationEvent = ConstructIntegrationEvent(integrationEventType, constructor);

        IntegrationEvent.ValidateRouteMetadata(integrationEvent.Topic, integrationEvent.Tag);
        IntegrationEvent.ValidateRouteMetadata(verificationEvent.Topic, verificationEvent.Tag);
        if (!StringComparer.Ordinal.Equals(integrationEvent.Topic, verificationEvent.Topic) ||
            !StringComparer.Ordinal.Equals(integrationEvent.Tag, verificationEvent.Tag))
        {
            throw new InvalidOperationException(
                $"Integration event type '{integrationEventType.FullName}' must produce stable route metadata from its public parameterless constructor.");
        }

        return new EventBusRouteDefinition(integrationEventType, integrationEvent.Topic, integrationEvent.Tag);
    }

    internal static void ValidateIntegrationEventType(Type integrationEventType)
    {
        ArgumentNullException.ThrowIfNull(integrationEventType);

        if (!integrationEventType.IsClass || integrationEventType.IsAbstract || integrationEventType.ContainsGenericParameters ||
            !typeof(IntegrationEvent).IsAssignableFrom(integrationEventType))
        {
            throw new ArgumentException(
                $"Type '{integrationEventType.FullName}' must be a concrete {nameof(IntegrationEvent)} type.",
                nameof(integrationEventType));
        }
    }

    private static IntegrationEvent ConstructIntegrationEvent(Type integrationEventType, ConstructorInfo constructor)
    {
        try
        {
            return (IntegrationEvent)constructor.Invoke(null);
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException(
                $"Integration event type '{integrationEventType.FullName}' could not be constructed while discovering its route.",
                exception);
        }
    }
}
