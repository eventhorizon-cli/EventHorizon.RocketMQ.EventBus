namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Support.Factories;

internal static class GrpcSendReceiptFactory
{
    private static readonly ConstructorInfo Constructor = typeof(GrpcSendReceipt).GetConstructor(
        BindingFlags.Instance | BindingFlags.NonPublic,
        binder: null,
        [typeof(string), typeof(long), typeof(string), typeof(string), typeof(IEnumerable<Uri>)],
        modifiers: null) ?? throw new InvalidOperationException("Unable to find the gRPC send receipt constructor.");

    internal static GrpcSendReceipt Create() => (GrpcSendReceipt)Constructor.Invoke(
        ["message-id", 0L, null, null, new[] { new Uri("http://127.0.0.1:8081") }]);
}
