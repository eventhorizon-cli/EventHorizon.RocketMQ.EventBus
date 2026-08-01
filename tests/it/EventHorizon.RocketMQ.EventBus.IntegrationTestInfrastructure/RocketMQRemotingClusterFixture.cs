using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure.Infrastructure;
using Xunit;

namespace EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;

/// <summary>
/// Starts a disposable NameServer and direct-Broker topology for Remoting integration tests.
/// </summary>
/// <remarks>
/// Each Broker advertises a distinct loopback address and port that are reachable by the test process. A Proxy could
/// not use those loopback routes from inside Docker, so this fixture remains separate from
/// <see cref="RocketMQGrpcClusterFixture"/>.
/// </remarks>
public sealed class RocketMQRemotingClusterFixture : IAsyncLifetime
{
    private const string Image = "apache/rocketmq:5.5.0";
    private const int NameServerPort = 9876;
    private const string BrokerA = "eventbus-remoting-broker-a";
    private const string BrokerB = "eventbus-remoting-broker-b";
    private const string BrokerC = "eventbus-remoting-broker-c";
    private static readonly string[] BrokerNames = [BrokerA, BrokerB, BrokerC];

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly HostPortReservation _ports = HostPortReservation.Reserve(4);
    private readonly int _nameServerHostPort;
    private readonly int _brokerAHostPort;
    private readonly int _brokerBHostPort;
    private readonly int _brokerCHostPort;
    private readonly IContainer _nameServer;
    private readonly IContainer _brokerA;
    private readonly IContainer _brokerB;
    private readonly IContainer _brokerC;

    /// <summary>
    /// Gets the Topic prepared on every Broker for the Remoting EventBus suite.
    /// </summary>
    public static readonly string Topic = $"eventbus-remoting-push-it-{Guid.NewGuid():N}";

    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQRemotingClusterFixture"/> class.
    /// </summary>
    public RocketMQRemotingClusterFixture()
    {
        _nameServerHostPort = _ports[0];
        _brokerAHostPort = _ports[1];
        _brokerBHostPort = _ports[2];
        _brokerCHostPort = _ports[3];

        _nameServer = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("eventbus-remoting-nameserver")
            .WithPortBinding(_nameServerHostPort, NameServerPort)
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithCommand("sh", "mqnamesrv")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(NameServerPort))
            .Build();

        _brokerA = CreateBroker(BrokerA, _brokerAHostPort);
        _brokerB = CreateBroker(BrokerB, _brokerBHostPort);
        _brokerC = CreateBroker(BrokerC, _brokerCHostPort);
    }

    /// <summary>
    /// Gets the host-reachable NameServer address used by classic Remoting clients.
    /// </summary>
    public string NameServerAddress => $"127.0.0.1:{_nameServerHostPort}";

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync().ConfigureAwait(false);
        await _nameServer.StartAsync().ConfigureAwait(false);
        await Task.WhenAll(_brokerA.StartAsync(), _brokerB.StartAsync(), _brokerC.StartAsync()).ConfigureAwait(false);
        await WaitForBrokersAsync().ConfigureAwait(false);
        await CreateTopicAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _brokerC.DisposeAsync().ConfigureAwait(false);
            await _brokerB.DisposeAsync().ConfigureAwait(false);
            await _brokerA.DisposeAsync().ConfigureAwait(false);
            await _nameServer.DisposeAsync().ConfigureAwait(false);
            await _network.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _ports.Dispose();
        }
    }

    private IContainer CreateBroker(string brokerName, int hostPort)
    {
        var configuration = Encoding.UTF8.GetBytes(
            "brokerClusterName=DefaultCluster\n" +
            $"brokerName={brokerName}\n" +
            "brokerId=0\n" +
            "brokerIP1=127.0.0.1\n" +
            $"namesrvAddr=eventbus-remoting-nameserver:{NameServerPort}\n" +
            $"listenPort={hostPort}\n" +
            "autoCreateTopicEnable=true\n" +
            "autoCreateSubscriptionGroup=true\n");

        return new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(brokerName)
            .WithPortBinding(hostPort, hostPort)
            .WithEnvironment("NAMESRV_ADDR", $"eventbus-remoting-nameserver:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithResourceMapping(configuration, "/tmp/eventbus-broker.conf")
            .WithCommand("sh", "mqbroker", "-c", "/tmp/eventbus-broker.conf")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(hostPort))
            .Build();
    }

    private async Task WaitForBrokersAsync()
    {
        ExecResult result = default;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            result = await _brokerA.ExecAsync(
                ["sh", "mqadmin", "clusterList", "-n", $"eventbus-remoting-nameserver:{NameServerPort}"])
                .ConfigureAwait(false);
            if (result.ExitCode == 0 && BrokerNames.All(result.Stdout.Contains))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"The Remoting fixture Brokers did not all register. stdout: {result.Stdout} stderr: {result.Stderr}");
    }

    private async Task CreateTopicAsync()
    {
        var brokers = new[]
        {
            (BrokerA, _brokerAHostPort),
            (BrokerB, _brokerBHostPort),
            (BrokerC, _brokerCHostPort),
        };
        foreach (var (brokerName, port) in brokers)
        {
            var result = await _brokerA.ExecAsync(
                [
                    "sh", "mqadmin", "updateTopic",
                    "-n", $"eventbus-remoting-nameserver:{NameServerPort}",
                    "-b", $"{brokerName}:{port}",
                    "-t", Topic,
                    "-r", "3",
                    "-w", "3",
                    "-a", "+message.type=NORMAL",
                ])
                .ConfigureAwait(false);
            if (result.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to create Remoting EventBus Topic on '{brokerName}'. stdout: {result.Stdout} stderr: {result.Stderr}");
            }
        }

        await WaitForTopicRouteAsync().ConfigureAwait(false);
    }

    private async Task WaitForTopicRouteAsync()
    {
        ExecResult result = default;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            result = await _brokerA.ExecAsync(
                ["sh", "mqadmin", "topicRoute", "-n", $"eventbus-remoting-nameserver:{NameServerPort}", "-t", Topic])
                .ConfigureAwait(false);
            if (result.ExitCode == 0 && BrokerNames.All(result.Stdout.Contains))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"The Remoting EventBus Topic route is incomplete. stdout: {result.Stdout} stderr: {result.Stderr}");
    }

}
