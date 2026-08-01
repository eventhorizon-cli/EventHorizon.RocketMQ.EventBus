using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests;

/// <summary>
/// Exercises the gRPC EventBus adapter against a real cluster-mode Proxy and three independent Brokers.
/// </summary>
[Collection(RocketMQGrpcCollection.Name)]
public sealed class GrpcEventBusPushIntegrationTests(RocketMQGrpcClusterFixture fixture)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushEventBus_PublishesAndDispatchesTaggedAndUntaggedEventsOneMessageAtATime()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var taggedIds = CreateDeliveryIds();
        var untaggedIds = CreateDeliveryIds();
        var recorder = new GrpcPushDeliveryRecorder(taggedIds, untaggedIds);
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(recorder);
        builder.Services
            .AddRocketMQGrpc(options =>
            {
                options.Endpoint = fixture.Endpoint;
                options.RequestTimeout = TimeSpan.FromSeconds(10);
            })
            .AddGrpcEventBus(
                configureConsumer: options =>
                {
                    options.GroupName = $"eventbus-grpc-it-{Guid.NewGuid():N}";
                    options.MaxConcurrency = 4;
                    options.BatchSize = 4;
                    options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                },
                configureProducer: options => options.Topics.Add(RocketMQGrpcClusterFixture.Topic))
            .AddHandler<GrpcTaggedPushHandler>()
            .AddHandler<GrpcUntaggedPushHandler>();

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);
        try
        {
            var eventBus = host.Services.GetRequiredService<IEventBus>();
            await Task.WhenAll(
                taggedIds.Select(deliveryId => eventBus.PublishAsync(
                    new GrpcTaggedIntegrationEvent { DeliveryId = deliveryId }, cancellationToken))
                    .Concat(untaggedIds.Select(deliveryId => eventBus.PublishAsync(
                        new GrpcUntaggedIntegrationEvent { DeliveryId = deliveryId }, cancellationToken))));

            await recorder.WaitForExpectedDeliveriesAsync(TimeSpan.FromSeconds(45), cancellationToken);
            var brokerOffsets = await fixture.WaitForMessagesOnAllBrokersAsync(
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
}
