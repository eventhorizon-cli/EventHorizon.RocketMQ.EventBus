using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests;

/// <summary>
/// Defines the collection that owns the disposable three-Broker gRPC topology.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class RocketMQGrpcCollection : ICollectionFixture<RocketMQGrpcClusterFixture>
{
    /// <summary>
    /// Gets the collection name used by gRPC EventBus integration tests.
    /// </summary>
    public const string Name = "EventBus gRPC three-Broker integration";
}
