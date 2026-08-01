using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests;

/// <summary>
/// Defines the collection that owns the disposable three-Broker Remoting topology.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class RocketMQRemotingCollection : ICollectionFixture<RocketMQRemotingClusterFixture>
{
    /// <summary>
    /// Gets the collection name used by Remoting EventBus integration tests.
    /// </summary>
    public const string Name = "EventBus Remoting three-Broker integration";
}
