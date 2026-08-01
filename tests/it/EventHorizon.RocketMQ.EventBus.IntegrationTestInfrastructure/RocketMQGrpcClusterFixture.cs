using System.Globalization;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure.Infrastructure;
using Xunit;

namespace EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;

/// <summary>
/// Starts a disposable RocketMQ cluster-mode Proxy topology for gRPC integration tests.
/// </summary>
/// <remarks>
/// The topology has one NameServer and three independent master Brokers. The Brokers advertise Docker-network aliases
/// because the Proxy, not the test process, resolves their routes. This is intentionally separate from
/// <see cref="RocketMQRemotingClusterFixture"/>.
/// </remarks>
public sealed class RocketMQGrpcClusterFixture : IAsyncLifetime
{
    private const string Image = "apache/rocketmq:5.5.0";
    private const int NameServerPort = 9876;
    private const int BrokerPort = 10911;
    private const int ProxyPort = 8081;
    private const string BrokerA = "eventbus-grpc-broker-a";
    private const string BrokerB = "eventbus-grpc-broker-b";
    private const string BrokerC = "eventbus-grpc-broker-c";
    private static readonly string[] BrokerNames = [BrokerA, BrokerB, BrokerC];

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly HostPortReservation _ports = HostPortReservation.Reserve(1);
    private readonly IContainer _nameServer;
    private readonly IContainer _brokerA;
    private readonly IContainer _brokerB;
    private readonly IContainer _brokerC;
    private readonly IContainer _proxy;

    /// <summary>
    /// Gets the Topic prepared on every Broker for the gRPC EventBus suite.
    /// </summary>
    public static readonly string Topic = $"eventbus-grpc-push-it-{Guid.NewGuid():N}";

    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQGrpcClusterFixture"/> class.
    /// </summary>
    public RocketMQGrpcClusterFixture()
    {
        _nameServer = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("eventbus-grpc-nameserver")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithCommand("sh", "mqnamesrv")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(NameServerPort))
            .Build();

        _brokerA = CreateBroker(BrokerA);
        _brokerB = CreateBroker(BrokerB);
        _brokerC = CreateBroker(BrokerC);

        var proxyConfiguration = Encoding.UTF8.GetBytes(
            "{\n" +
            "  \"rocketMQClusterName\": \"DefaultCluster\",\n" +
            "  \"proxyClusterName\": \"DefaultCluster\",\n" +
            $"  \"grpcServerPort\": {ProxyPort},\n" +
            "  \"useEndpointPortFromRequest\": true\n" +
            "}\n");
        _proxy = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("eventbus-grpc-proxy")
            .WithPortBinding(_ports[0], ProxyPort)
            .WithEnvironment("NAMESRV_ADDR", $"eventbus-grpc-nameserver:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms512m -Xmx512m")
            .WithResourceMapping(proxyConfiguration, "/home/rocketmq/rocketmq-5.5.0/conf/rmq-proxy.json")
            .WithCommand(
                "sh",
                "mqproxy",
                "-pm",
                "cluster",
                "-n",
                $"eventbus-grpc-nameserver:{NameServerPort}",
                "-pc",
                "/home/rocketmq/rocketmq-5.5.0/conf/rmq-proxy.json")
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilInternalTcpPortIsAvailable(ProxyPort)
                    .UntilMessageIsLogged("rocketmq-proxy startup successfully"))
            .Build();
    }

    /// <summary>
    /// Gets the test-process endpoint for the cluster-mode gRPC Proxy.
    /// </summary>
    public string Endpoint => $"{_proxy.Hostname}:{_proxy.GetMappedPublicPort(ProxyPort)}";

    /// <summary>
    /// Waits until the EventBus test Topic has a positive maximum offset on every Broker.
    /// </summary>
    /// <param name="timeout">The maximum time allowed for broker offsets to become visible.</param>
    /// <param name="cancellationToken">The token used to cancel the wait.</param>
    /// <returns>The aggregate maximum offsets keyed by Broker name.</returns>
    public async Task<IReadOnlyDictionary<string, long>> WaitForMessagesOnAllBrokersAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        IReadOnlyDictionary<string, long> offsets;
        do
        {
            offsets = await GetBrokerMaxOffsetsAsync(cancellationToken).ConfigureAwait(false);
            if (BrokerNames.All(brokerName => offsets.TryGetValue(brokerName, out var offset) && offset > 0))
            {
                return offsets;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
        }
        while (DateTimeOffset.UtcNow < deadline);

        return await GetBrokerMaxOffsetsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync().ConfigureAwait(false);
        await _nameServer.StartAsync().ConfigureAwait(false);
        await Task.WhenAll(_brokerA.StartAsync(), _brokerB.StartAsync(), _brokerC.StartAsync()).ConfigureAwait(false);
        await WaitForBrokersAsync().ConfigureAwait(false);
        await CreateTopicAsync().ConfigureAwait(false);
        await _proxy.StartAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _proxy.DisposeAsync().ConfigureAwait(false);
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

    private IContainer CreateBroker(string brokerName)
    {
        var configuration = Encoding.UTF8.GetBytes(
            "brokerClusterName=DefaultCluster\n" +
            $"brokerName={brokerName}\n" +
            "brokerId=0\n" +
            $"brokerIP1={brokerName}\n" +
            $"namesrvAddr=eventbus-grpc-nameserver:{NameServerPort}\n" +
            $"listenPort={BrokerPort}\n" +
            "autoCreateTopicEnable=true\n" +
            "autoCreateSubscriptionGroup=true\n");

        return new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(brokerName)
            .WithEnvironment("NAMESRV_ADDR", $"eventbus-grpc-nameserver:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithResourceMapping(configuration, "/tmp/eventbus-broker.conf")
            .WithCommand("sh", "mqbroker", "-c", "/tmp/eventbus-broker.conf")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(BrokerPort))
            .Build();
    }

    private async Task WaitForBrokersAsync()
    {
        ExecResult result = default;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            result = await _brokerA.ExecAsync(
                ["sh", "mqadmin", "clusterList", "-n", $"eventbus-grpc-nameserver:{NameServerPort}"])
                .ConfigureAwait(false);
            if (result.ExitCode == 0 && BrokerNames.All(result.Stdout.Contains))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"The gRPC fixture Brokers did not all register. stdout: {result.Stdout} stderr: {result.Stderr}");
    }

    private async Task CreateTopicAsync()
    {
        foreach (var brokerName in BrokerNames)
        {
            var result = await _brokerA.ExecAsync(
                [
                    "sh", "mqadmin", "updateTopic",
                    "-n", $"eventbus-grpc-nameserver:{NameServerPort}",
                    "-b", $"{brokerName}:{BrokerPort}",
                    "-t", Topic,
                    "-r", "3",
                    "-w", "3",
                    "-a", "+message.type=NORMAL",
                ])
                .ConfigureAwait(false);
            if (result.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to create gRPC EventBus Topic on '{brokerName}'. stdout: {result.Stdout} stderr: {result.Stderr}");
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
                ["sh", "mqadmin", "topicRoute", "-n", $"eventbus-grpc-nameserver:{NameServerPort}", "-t", Topic])
                .ConfigureAwait(false);
            if (result.ExitCode == 0 && BrokerNames.All(result.Stdout.Contains))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"The gRPC EventBus Topic route is incomplete. stdout: {result.Stdout} stderr: {result.Stderr}");
    }

    private async Task<IReadOnlyDictionary<string, long>> GetBrokerMaxOffsetsAsync(CancellationToken cancellationToken)
    {
        var result = await _brokerA.ExecAsync(
            ["sh", "mqadmin", "topicStatus", "-n", $"eventbus-grpc-nameserver:{NameServerPort}", "-t", Topic],
            cancellationToken)
            .ConfigureAwait(false);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Unable to inspect gRPC EventBus Topic offsets. stdout: {result.Stdout} stderr: {result.Stderr}");
        }

        var offsets = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var line in result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var columns = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
            if (columns.Length < 4 ||
                !BrokerNames.Contains(columns[0], StringComparer.Ordinal) ||
                !long.TryParse(columns[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var maxOffset))
            {
                continue;
            }

            offsets.TryGetValue(columns[0], out var currentOffset);
            offsets[columns[0]] = currentOffset + maxOffset;
        }

        return offsets;
    }
}
