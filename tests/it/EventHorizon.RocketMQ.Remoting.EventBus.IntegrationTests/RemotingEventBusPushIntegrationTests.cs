using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Admin;
using EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests;

/// <summary>
/// Exercises the Remoting EventBus adapter against a real NameServer and three direct Brokers.
/// </summary>
[Collection(RocketMQRemotingCollection.Name)]
public sealed class RemotingEventBusPushIntegrationTests(RocketMQRemotingClusterFixture fixture)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushEventBus_PublishesAndDispatchesTaggedAndUntaggedEventsOneMessageAtATime()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var taggedIds = CreateDeliveryIds();
        var untaggedIds = CreateDeliveryIds();
        var recorder = new RemotingPushDeliveryRecorder(taggedIds, untaggedIds);
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(recorder);
        var rocketMQ = builder.Services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = fixture.NameServerAddress;
            options.RequestTimeout = TimeSpan.FromSeconds(10);
            options.NameServerRequestTimeout = TimeSpan.FromSeconds(10);
        });
        rocketMQ
            .AddRemotingAdmin()
            .AddRemotingEventBus(
                configureConsumer: options =>
                {
                    options.GroupName = $"eventbus-remoting-it-{Guid.NewGuid():N}";
                    options.MaxConcurrency = 4;
                    options.BatchSize = 4;
                    options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                },
                configureProducer: options => options.GroupName = $"eventbus-remoting-publisher-{Guid.NewGuid():N}")
            .AddHandler<RemotingTaggedPushHandler>()
            .AddHandler<RemotingUntaggedPushHandler>();

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);
        try
        {
            var eventBus = host.Services.GetRequiredService<IEventBus>();
            await Task.WhenAll(
                taggedIds.Select(deliveryId => eventBus.PublishAsync(
                    new RemotingTaggedIntegrationEvent { DeliveryId = deliveryId }, cancellationToken))
                    .Concat(untaggedIds.Select(deliveryId => eventBus.PublishAsync(
                        new RemotingUntaggedIntegrationEvent { DeliveryId = deliveryId }, cancellationToken))));

            await recorder.WaitForExpectedDeliveriesAsync(TimeSpan.FromSeconds(45), cancellationToken);
            var brokerOffsets = await WaitForMessagesOnAllBrokersAsync(
                host.Services.GetRequiredService<IRemotingAdmin>(),
                TimeSpan.FromSeconds(15),
                cancellationToken);
            Assert.Equal(3, brokerOffsets.Count(static entry => entry.Value > 0));

            foreach (var deliveryId in taggedIds)
            {
                Assert.Equal(1, recorder.GetTaggedDeliveryCount(deliveryId));
                Assert.Equal(0, recorder.GetUntaggedDeliveryCount(deliveryId));
            }

            foreach (var deliveryId in untaggedIds)
            {
                Assert.Equal(1, recorder.GetUntaggedDeliveryCount(deliveryId));
                Assert.Equal(0, recorder.GetTaggedDeliveryCount(deliveryId));
            }
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    private static Guid[] CreateDeliveryIds() => Enumerable.Range(0, 12).Select(static _ => Guid.NewGuid()).ToArray();

    private static async Task<IReadOnlyDictionary<string, long>> WaitForMessagesOnAllBrokersAsync(
        IRemotingAdmin admin,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        IReadOnlyDictionary<string, long> offsets;
        do
        {
            offsets = await GetBrokerMaxOffsetsAsync(admin, cancellationToken);
            if (offsets.Count == 3 && offsets.Values.All(static offset => offset > 0))
            {
                return offsets;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        while (DateTimeOffset.UtcNow < deadline);

        return await GetBrokerMaxOffsetsAsync(admin, cancellationToken);
    }

    private static async Task<IReadOnlyDictionary<string, long>> GetBrokerMaxOffsetsAsync(
        IRemotingAdmin admin,
        CancellationToken cancellationToken)
    {
        var queues = await admin.GetMessageQueuesAsync(RocketMQRemotingClusterFixture.Topic, cancellationToken);
        var queueOffsets = await Task.WhenAll(queues.Select(async queue => new
        {
            queue.BrokerName,
            Offset = await admin.GetMaxOffsetAsync(queue, cancellationToken: cancellationToken),
        }));

        return queueOffsets
            .GroupBy(static entry => entry.BrokerName, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => group.Sum(static entry => entry.Offset),
                StringComparer.Ordinal);
    }
}
