using System.Reflection;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Registration;

public sealed class RemotingEventBusOptionsTests
{
    [Fact]
    public void RemotingEventBusConsumerOptions_PublicProperties_ExposeOnlyTheCuratedSurface()
    {
        AssertPublicProperties(
            typeof(RemotingEventBusConsumerOptions),
            new Dictionary<string, Type>(StringComparer.Ordinal)
            {
                [nameof(RemotingEventBusConsumerOptions.GroupName)] = typeof(string),
                [nameof(RemotingEventBusConsumerOptions.InitialPosition)] = typeof(ConsumeFromPosition),
                [nameof(RemotingEventBusConsumerOptions.ConsumeTimestamp)] = typeof(DateTimeOffset?),
                [nameof(RemotingEventBusConsumerOptions.MaxConcurrency)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.PullBatchSize)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.PopBatchSize)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.PopInvisibleDuration)] = typeof(TimeSpan),
                [nameof(RemotingEventBusConsumerOptions.PopMaxInflightMessagesPerAssignment)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.PullMaxCachedMessages)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.PullMaxCachedMessageBytes)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.MaxMessageBytes)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.MaxDeliveryAttempts)] = typeof(int),
                [nameof(RemotingEventBusConsumerOptions.LongPollingTimeout)] = typeof(TimeSpan),
                [nameof(RemotingEventBusConsumerOptions.RetryDelay)] = typeof(TimeSpan),
                [nameof(RemotingEventBusConsumerOptions.ConsumeTimeout)] = typeof(TimeSpan),
                [nameof(RemotingEventBusConsumerOptions.QueueAssignmentMode)] =
                    typeof(RemotingPushQueueAssignmentMode),
                [nameof(RemotingEventBusConsumerOptions.SkipDeserializationFailures)] = typeof(bool),
            });
    }

    [Fact]
    public void RemotingEventBusProducerOptions_PublicProperties_ExposeOnlyTheCuratedSurface()
    {
        AssertPublicProperties(
            typeof(RemotingEventBusProducerOptions),
            new Dictionary<string, Type>(StringComparer.Ordinal)
            {
                [nameof(RemotingEventBusProducerOptions.GroupName)] = typeof(string),
                [nameof(RemotingEventBusProducerOptions.DefaultTopicQueueNums)] = typeof(int),
                [nameof(RemotingEventBusProducerOptions.SendMsgTimeout)] = typeof(TimeSpan),
                [nameof(RemotingEventBusProducerOptions.CompressMsgBodyOverHowmuch)] = typeof(int),
                [nameof(RemotingEventBusProducerOptions.RetryTimesWhenSendFailed)] = typeof(int),
                [nameof(RemotingEventBusProducerOptions.MaxMessageSize)] = typeof(int),
            });
    }

    [Fact]
    public async Task AddRemotingEventBus_MapsEveryConsumerOptionAndForcesConcurrentSingleMessageConsumption()
    {
        var services = new ServiceCollection();
        RemotingPushConsumerOptions? capturedOptions = null;
        var timestamp = new DateTimeOffset(2026, 8, 10, 12, 34, 56, TimeSpan.Zero);
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options =>
            {
                options.GroupName = "mapped-consumer";
                options.InitialPosition = ConsumeFromPosition.Timestamp;
                options.ConsumeTimestamp = timestamp;
                options.MaxConcurrency = 7;
                options.PullBatchSize = 13;
                options.PopBatchSize = 9;
                options.PopInvisibleDuration = TimeSpan.FromSeconds(45);
                options.PopMaxInflightMessagesPerAssignment = 77;
                options.PullMaxCachedMessages = 123;
                options.PullMaxCachedMessageBytes = 456_789;
                options.MaxMessageBytes = 567_890;
                options.MaxDeliveryAttempts = 11;
                options.LongPollingTimeout = TimeSpan.FromSeconds(23);
                options.RetryDelay = TimeSpan.FromSeconds(4);
                options.ConsumeTimeout = TimeSpan.FromMinutes(7);
                options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
                options.SkipDeserializationFailures = false;
            })
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();

        var options = Assert.IsType<RemotingPushConsumerOptions>(capturedOptions);
        Assert.Equal("mapped-consumer", options.GroupName);
        Assert.Equal(ConsumeFromPosition.Timestamp, options.InitialPosition);
        Assert.Equal(timestamp, options.ConsumeTimestamp);
        Assert.Equal(7, options.MaxConcurrency);
        Assert.Equal(13, options.PullBatchSize);
        Assert.Equal(9, options.PopBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(45), options.PopInvisibleDuration);
        Assert.Equal(77, options.PopMaxInflightMessagesPerAssignment);
        Assert.Equal(123, options.PullMaxCachedMessages);
        Assert.Equal(456_789, options.PullMaxCachedMessageBytes);
        Assert.Equal(567_890, options.MaxMessageBytes);
        Assert.Equal(11, options.MaxDeliveryAttempts);
        Assert.Equal(TimeSpan.FromSeconds(23), options.LongPollingTimeout);
        Assert.Equal(TimeSpan.FromSeconds(4), options.RetryDelay);
        Assert.Equal(TimeSpan.FromMinutes(7), options.ConsumeTimeout);
        Assert.Equal(RemotingPushQueueAssignmentMode.Broker, options.QueueAssignmentMode);
        Assert.Equal(ConsumerMode.Clustering, options.ConsumerMode);
        Assert.False(options.ConsumeOrderly);
        Assert.Equal(1, options.ConsumeMessageBatchSize);
        Assert.Null(options.LocalOffsetStorePath);
        Assert.Equal(-1, options.OrderlyMaxReconsumeTimes);
    }

    [Fact]
    public async Task AddRemotingEventBus_MapsEveryProducerOptionAndLeavesTransactionDefaultsUntouched()
    {
        var services = new ServiceCollection();
        RemotingProducerOptions? capturedOptions = null;
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(
                configureProducer: options =>
                {
                    options.GroupName = "mapped-producer";
                    options.DefaultTopicQueueNums = 9;
                    options.SendMsgTimeout = TimeSpan.FromSeconds(17);
                    options.CompressMsgBodyOverHowmuch = 8_192;
                    options.RetryTimesWhenSendFailed = 5;
                    options.MaxMessageSize = 3 * 1024 * 1024;
                });
        services.PostConfigureAll<RemotingProducerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingProducer>();

        var options = Assert.IsType<RemotingProducerOptions>(capturedOptions);
        Assert.Equal("mapped-producer", options.GroupName);
        Assert.Equal(9, options.DefaultTopicQueueNums);
        Assert.Equal(TimeSpan.FromSeconds(17), options.SendMsgTimeout);
        Assert.Equal(8_192, options.CompressMsgBodyOverHowmuch);
        Assert.Equal(5, options.RetryTimesWhenSendFailed);
        Assert.Equal(3 * 1024 * 1024, options.MaxMessageSize);
        Assert.Null(options.LocalTransactionExecutor);
        Assert.Null(options.TransactionChecker);
        Assert.Equal(Environment.ProcessorCount, options.MaxConcurrentTransactionChecks);
    }

    [Fact]
    public async Task AddRemotingEventBus_UsesConsumerWrapperDefaultsForMappedOptions()
    {
        var services = new ServiceCollection();
        RemotingPushConsumerOptions? capturedOptions = null;
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options => options.GroupName = "default-consumer")
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();

        var options = Assert.IsType<RemotingPushConsumerOptions>(capturedOptions);
        Assert.Equal("default-consumer", options.GroupName);
        Assert.Equal(ConsumeFromPosition.End, options.InitialPosition);
        Assert.Null(options.ConsumeTimestamp);
        Assert.Equal(Environment.ProcessorCount, options.MaxConcurrency);
        Assert.Equal(32, options.PullBatchSize);
        Assert.Equal(32, options.PopBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(60), options.PopInvisibleDuration);
        Assert.Equal(96, options.PopMaxInflightMessagesPerAssignment);
        Assert.Equal(1024, options.PullMaxCachedMessages);
        Assert.Equal(32 * 1024 * 1024, options.PullMaxCachedMessageBytes);
        Assert.Equal(32 * 1024 * 1024, options.MaxMessageBytes);
        Assert.Equal(16, options.MaxDeliveryAttempts);
        Assert.Equal(TimeSpan.FromSeconds(15), options.LongPollingTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), options.RetryDelay);
        Assert.Equal(TimeSpan.FromMinutes(15), options.ConsumeTimeout);
        Assert.Equal(RemotingPushQueueAssignmentMode.Client, options.QueueAssignmentMode);
        Assert.Equal(ConsumerMode.Clustering, options.ConsumerMode);
        Assert.False(options.ConsumeOrderly);
        Assert.Equal(1, options.ConsumeMessageBatchSize);
    }

    [Fact]
    public async Task AddRemotingEventBus_UsesProducerWrapperDefaultsForMappedOptions()
    {
        var services = new ServiceCollection();
        RemotingProducerOptions? capturedOptions = null;
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(configureProducer: static _ => { });
        services.PostConfigureAll<RemotingProducerOptions>(options => capturedOptions = options);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingProducer>();

        var options = Assert.IsType<RemotingProducerOptions>(capturedOptions);
        Assert.Equal("DEFAULT_PRODUCER", options.GroupName);
        Assert.Equal(4, options.DefaultTopicQueueNums);
        Assert.Equal(TimeSpan.FromSeconds(3), options.SendMsgTimeout);
        Assert.Equal(4 * 1024, options.CompressMsgBodyOverHowmuch);
        Assert.Equal(2, options.RetryTimesWhenSendFailed);
        Assert.Equal(4 * 1024 * 1024, options.MaxMessageSize);
        Assert.Null(options.LocalTransactionExecutor);
        Assert.Null(options.TransactionChecker);
        Assert.Equal(Environment.ProcessorCount, options.MaxConcurrentTransactionChecks);
    }

    [Fact]
    public async Task AddRemotingEventBus_SnapshotsConsumerWrapperBeforeCallerMutation()
    {
        var services = new ServiceCollection();
        RemotingEventBusConsumerOptions? wrapper = null;
        RemotingPushConsumerOptions? capturedOptions = null;
        var timestamp = new DateTimeOffset(2026, 8, 10, 1, 2, 3, TimeSpan.Zero);
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(options =>
            {
                wrapper = options;
                options.GroupName = "original-consumer";
                options.InitialPosition = ConsumeFromPosition.Timestamp;
                options.ConsumeTimestamp = timestamp;
                options.MaxConcurrency = 3;
                options.PullBatchSize = 12;
                options.PopBatchSize = 8;
                options.PopInvisibleDuration = TimeSpan.FromSeconds(30);
                options.PopMaxInflightMessagesPerAssignment = 48;
                options.PullMaxCachedMessages = 256;
                options.PullMaxCachedMessageBytes = 128 * 1024;
                options.MaxMessageBytes = 256 * 1024;
                options.MaxDeliveryAttempts = 4;
                options.LongPollingTimeout = TimeSpan.FromSeconds(6);
                options.RetryDelay = TimeSpan.FromSeconds(2);
                options.ConsumeTimeout = TimeSpan.FromMinutes(2);
                options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
                options.SkipDeserializationFailures = false;
            })
            .AddHandler<RemotingTestHandler>();
        services.PostConfigureAll<RemotingPushConsumerOptions>(options => capturedOptions = options);

        Assert.NotNull(wrapper);
        var callerOptions = wrapper!;
        callerOptions.GroupName = "mutated-consumer";
        callerOptions.InitialPosition = ConsumeFromPosition.Beginning;
        callerOptions.ConsumeTimestamp = timestamp.AddDays(1);
        callerOptions.MaxConcurrency = 17;
        callerOptions.PullBatchSize = 27;
        callerOptions.PopBatchSize = 16;
        callerOptions.PopInvisibleDuration = TimeSpan.FromMinutes(2);
        callerOptions.PopMaxInflightMessagesPerAssignment = 96;
        callerOptions.PullMaxCachedMessages = 512;
        callerOptions.PullMaxCachedMessageBytes = 512 * 1024;
        callerOptions.MaxMessageBytes = 512 * 1024;
        callerOptions.MaxDeliveryAttempts = 9;
        callerOptions.LongPollingTimeout = TimeSpan.FromSeconds(18);
        callerOptions.RetryDelay = TimeSpan.FromSeconds(8);
        callerOptions.ConsumeTimeout = TimeSpan.FromMinutes(9);
        callerOptions.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Client;
        callerOptions.SkipDeserializationFailures = true;

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingPushConsumer>();

        var options = Assert.IsType<RemotingPushConsumerOptions>(capturedOptions);
        Assert.Equal("original-consumer", options.GroupName);
        Assert.Equal(ConsumeFromPosition.Timestamp, options.InitialPosition);
        Assert.Equal(timestamp, options.ConsumeTimestamp);
        Assert.Equal(3, options.MaxConcurrency);
        Assert.Equal(12, options.PullBatchSize);
        Assert.Equal(8, options.PopBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(30), options.PopInvisibleDuration);
        Assert.Equal(48, options.PopMaxInflightMessagesPerAssignment);
        Assert.Equal(256, options.PullMaxCachedMessages);
        Assert.Equal(128 * 1024, options.PullMaxCachedMessageBytes);
        Assert.Equal(256 * 1024, options.MaxMessageBytes);
        Assert.Equal(4, options.MaxDeliveryAttempts);
        Assert.Equal(TimeSpan.FromSeconds(6), options.LongPollingTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), options.RetryDelay);
        Assert.Equal(TimeSpan.FromMinutes(2), options.ConsumeTimeout);
        Assert.Equal(RemotingPushQueueAssignmentMode.Broker, options.QueueAssignmentMode);
    }

    [Fact]
    public async Task AddRemotingEventBus_SnapshotsProducerWrapperBeforeCallerMutation()
    {
        var services = new ServiceCollection();
        RemotingEventBusProducerOptions? wrapper = null;
        RemotingProducerOptions? capturedOptions = null;
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(
                configureProducer: options =>
                {
                    wrapper = options;
                    options.GroupName = "original-producer";
                    options.DefaultTopicQueueNums = 7;
                    options.SendMsgTimeout = TimeSpan.FromSeconds(11);
                    options.CompressMsgBodyOverHowmuch = 6_144;
                    options.RetryTimesWhenSendFailed = 4;
                    options.MaxMessageSize = 2 * 1024 * 1024;
                });
        services.PostConfigureAll<RemotingProducerOptions>(options => capturedOptions = options);

        Assert.NotNull(wrapper);
        var callerOptions = wrapper!;
        callerOptions.GroupName = "mutated-producer";
        callerOptions.DefaultTopicQueueNums = 15;
        callerOptions.SendMsgTimeout = TimeSpan.FromSeconds(19);
        callerOptions.CompressMsgBodyOverHowmuch = 12_288;
        callerOptions.RetryTimesWhenSendFailed = 8;
        callerOptions.MaxMessageSize = 3 * 1024 * 1024;

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IRemotingProducer>();

        var options = Assert.IsType<RemotingProducerOptions>(capturedOptions);
        Assert.Equal("original-producer", options.GroupName);
        Assert.Equal(7, options.DefaultTopicQueueNums);
        Assert.Equal(TimeSpan.FromSeconds(11), options.SendMsgTimeout);
        Assert.Equal(6_144, options.CompressMsgBodyOverHowmuch);
        Assert.Equal(4, options.RetryTimesWhenSendFailed);
        Assert.Equal(2 * 1024 * 1024, options.MaxMessageSize);
    }

    private static void AssertPublicProperties(Type type, IReadOnlyDictionary<string, Type> expected)
    {
        var actual = type
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .ToDictionary(property => property.Name, property => property.PropertyType, StringComparer.Ordinal);

        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected.Keys.OrderBy(static name => name), actual.Keys.OrderBy(static name => name));
        foreach (var property in expected)
        {
            Assert.Equal(property.Value, actual[property.Key]);
        }
    }
}
