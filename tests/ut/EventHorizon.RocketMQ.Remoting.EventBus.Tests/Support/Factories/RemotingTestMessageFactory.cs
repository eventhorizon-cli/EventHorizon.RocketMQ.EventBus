using System.Reflection;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Support.Factories;

internal static class RemotingTestMessageFactory
{
    private static readonly ConstructorInfo MessageConstructor = typeof(RemotingMessageView).GetConstructor(
        BindingFlags.Instance | BindingFlags.NonPublic,
        binder: null,
        [
            typeof(string),
            typeof(byte[]),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(IReadOnlyList<string>),
            typeof(IReadOnlyDictionary<string, string>),
            typeof(int),
            typeof(string),
            typeof(int),
            typeof(string),
            typeof(long),
            typeof(long),
            typeof(DateTimeOffset),
            typeof(DateTimeOffset),
        ],
        modifiers: null) ?? throw new InvalidOperationException("Unable to find the Remoting message constructor.");

    private static readonly ConstructorInfo SendResultConstructor = typeof(RemotingSendResult).GetConstructor(
        BindingFlags.Instance | BindingFlags.NonPublic,
        binder: null,
        [
            typeof(RemotingSendStatus),
            typeof(string),
            typeof(string),
            typeof(RemotingMessageQueue),
            typeof(long),
            typeof(string),
            typeof(string),
            typeof(string),
            typeof(bool),
        ],
        modifiers: null) ?? throw new InvalidOperationException("Unable to find the Remoting send-result constructor.");

    internal static RemotingMessageView Create(
        string topic,
        string? tag,
        byte[] body,
        string? brokerName = null,
        int queueId = 0,
        long queueOffset = 0) =>
        (RemotingMessageView)MessageConstructor.Invoke(
        [
            topic,
            body,
            "message-id",
            "offset-message-id",
            tag,
            Array.Empty<string>(),
            new Dictionary<string, string>(StringComparer.Ordinal),
            1,
            null,
            queueId,
            brokerName,
            queueOffset,
            0L,
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch,
        ]);

    internal static byte[] Serialize(IntegrationEvent integrationEvent) =>
        new NewtonsoftJsonIntegrationEventSerializer().Serialize(integrationEvent);

    internal static RemotingSendResult CreateSendResult(RemotingSendStatus status) =>
        (RemotingSendResult)SendResultConstructor.Invoke(
        [
            status,
            "message-id",
            "offset-message-id",
            new RemotingMessageQueue("orders", "broker-a", 0),
            0L,
            null,
            null,
            "region-a",
            false,
        ]);
}
