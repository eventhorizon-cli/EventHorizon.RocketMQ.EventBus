namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Registration;

public sealed class GrpcEventBusOptionsTests
{
    [Fact]
    public void GrpcEventBusConsumerOptions_Defaults_MatchGrpcPushConsumerDefaults()
    {
        var eventBusOptions = new GrpcEventBusConsumerOptions();
        var grpcOptions = new GrpcPushConsumerOptions();

        Assert.Equal(grpcOptions.GroupName, eventBusOptions.GroupName);
        Assert.Equal(grpcOptions.MaxConcurrency, eventBusOptions.MaxConcurrency);
        Assert.Equal(grpcOptions.BatchSize, eventBusOptions.BatchSize);
        Assert.Equal(grpcOptions.MaxCachedMessages, eventBusOptions.MaxCachedMessages);
        Assert.Equal(grpcOptions.MaxCachedMessageBytes, eventBusOptions.MaxCachedMessageBytes);
        Assert.Equal(grpcOptions.MaxDeliveryAttempts, eventBusOptions.MaxDeliveryAttempts);
        Assert.Equal(grpcOptions.InvisibleDuration, eventBusOptions.InvisibleDuration);
        Assert.Equal(grpcOptions.ConsumeTimeout, eventBusOptions.ConsumeTimeout);
        Assert.Equal(grpcOptions.LongPollingTimeout, eventBusOptions.LongPollingTimeout);
        Assert.Equal(grpcOptions.RetryDelay, eventBusOptions.RetryDelay);
        Assert.True(eventBusOptions.SkipDeserializationFailures);
    }

    [Fact]
    public void GrpcEventBusProducerOptions_Defaults_MatchGrpcProducerDefaults()
    {
        var eventBusOptions = new GrpcEventBusProducerOptions();
        var grpcOptions = new GrpcProducerOptions();

        Assert.Equal(grpcOptions.SendMsgTimeout, eventBusOptions.SendMsgTimeout);
        Assert.Equal(grpcOptions.RetryTimesWhenSendFailed, eventBusOptions.RetryTimesWhenSendFailed);
        Assert.Equal(grpcOptions.MaxMessageSize, eventBusOptions.MaxMessageSize);
        Assert.Empty(grpcOptions.Topics);
        Assert.Null(grpcOptions.TransactionChecker);
        Assert.Equal(
            new GrpcProducerOptions().MaxConcurrentTransactionChecks,
            grpcOptions.MaxConcurrentTransactionChecks);
    }

    [Fact]
    public async Task AddGrpcEventBus_MapsEveryConsumerOptionToGrpcPushConsumerOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        GrpcPushConsumerOptions? capturedOptions = null;
        GrpcEventBusConsumerOptions? configuredOptions = null;
        services.PostConfigureAll<GrpcPushConsumerOptions>(options => capturedOptions = options);
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options =>
            {
                configuredOptions = options;
                options.GroupName = "mapped-consumer";
                options.MaxConcurrency = 7;
                options.BatchSize = 13;
                options.MaxCachedMessages = 17;
                options.MaxCachedMessageBytes = 19 * 1024;
                options.MaxDeliveryAttempts = 23;
                options.InvisibleDuration = TimeSpan.FromSeconds(29);
                options.ConsumeTimeout = TimeSpan.FromMinutes(31);
                options.LongPollingTimeout = TimeSpan.FromSeconds(37);
                options.RetryDelay = TimeSpan.FromSeconds(41);
                options.SkipDeserializationFailures = false;
            });
        eventBusBuilder.AddHandler<OrderCreatedHandler>();

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IGrpcPushConsumer>();

        var mappedOptions = Assert.IsType<GrpcPushConsumerOptions>(capturedOptions);
        var wrapper = Assert.IsType<GrpcEventBusConsumerOptions>(configuredOptions);
        Assert.Equal("mapped-consumer", mappedOptions.GroupName);
        Assert.Equal(7, mappedOptions.MaxConcurrency);
        Assert.Equal(13, mappedOptions.BatchSize);
        Assert.Equal(17, mappedOptions.MaxCachedMessages);
        Assert.Equal(19 * 1024, mappedOptions.MaxCachedMessageBytes);
        Assert.Equal(23, mappedOptions.MaxDeliveryAttempts);
        Assert.Equal(TimeSpan.FromSeconds(29), mappedOptions.InvisibleDuration);
        Assert.Equal(TimeSpan.FromMinutes(31), mappedOptions.ConsumeTimeout);
        Assert.Equal(TimeSpan.FromSeconds(37), mappedOptions.LongPollingTimeout);
        Assert.Equal(TimeSpan.FromSeconds(41), mappedOptions.RetryDelay);
        Assert.False(wrapper.SkipDeserializationFailures);
    }

    [Fact]
    public async Task AddGrpcEventBus_MapsEveryProducerOptionAndLeavesTransactionSettingsAtMainDefaults()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        GrpcProducerOptions? capturedOptions = null;
        GrpcEventBusProducerOptions? configuredOptions = null;
        services.PostConfigureAll<GrpcProducerOptions>(options => capturedOptions = options);
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(configureProducer: options =>
            {
                configuredOptions = options;
                options.SendMsgTimeout = TimeSpan.FromSeconds(43);
                options.RetryTimesWhenSendFailed = 47;
                options.MaxMessageSize = 53 * 1024;
            });

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IGrpcProducer>();

        var mappedOptions = Assert.IsType<GrpcProducerOptions>(capturedOptions);
        var wrapper = Assert.IsType<GrpcEventBusProducerOptions>(configuredOptions);
        Assert.Equal(TimeSpan.FromSeconds(43), mappedOptions.SendMsgTimeout);
        Assert.Equal(47, mappedOptions.RetryTimesWhenSendFailed);
        Assert.Equal(53 * 1024, mappedOptions.MaxMessageSize);
        Assert.Empty(mappedOptions.Topics);
        Assert.Null(mappedOptions.TransactionChecker);
        Assert.Equal(
            new GrpcProducerOptions().MaxConcurrentTransactionChecks,
            mappedOptions.MaxConcurrentTransactionChecks);
        Assert.Equal(TimeSpan.FromSeconds(43), wrapper.SendMsgTimeout);
        Assert.Equal(47, wrapper.RetryTimesWhenSendFailed);
        Assert.Equal(53 * 1024, wrapper.MaxMessageSize);
    }

    [Fact]
    public async Task AddGrpcEventBus_SnapshotsConsumerOptionsAtRegistration()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        GrpcPushConsumerOptions? capturedOptions = null;
        GrpcEventBusConsumerOptions? retainedOptions = null;
        services.PostConfigureAll<GrpcPushConsumerOptions>(options => capturedOptions = options);
        var eventBusBuilder = services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(options =>
            {
                retainedOptions = options;
                options.GroupName = "original-consumer";
                options.MaxConcurrency = 59;
                options.BatchSize = 61;
                options.MaxCachedMessages = 67;
                options.MaxCachedMessageBytes = 71 * 1024;
                options.MaxDeliveryAttempts = 73;
                options.InvisibleDuration = TimeSpan.FromSeconds(79);
                options.ConsumeTimeout = TimeSpan.FromMinutes(83);
                options.LongPollingTimeout = TimeSpan.FromSeconds(89);
                options.RetryDelay = TimeSpan.FromSeconds(97);
            });
        var wrapper = retainedOptions ?? throw new InvalidOperationException(
            "The consumer options delegate was not invoked.");
        eventBusBuilder.AddHandler<OrderCreatedHandler>();

        wrapper.GroupName = "mutated-consumer";
        wrapper.MaxConcurrency = 101;
        wrapper.BatchSize = 103;
        wrapper.MaxCachedMessages = 107;
        wrapper.MaxCachedMessageBytes = 109;
        wrapper.MaxDeliveryAttempts = 113;
        wrapper.InvisibleDuration = TimeSpan.FromSeconds(127);
        wrapper.ConsumeTimeout = TimeSpan.FromSeconds(131);
        wrapper.LongPollingTimeout = TimeSpan.FromSeconds(137);
        wrapper.RetryDelay = TimeSpan.FromSeconds(139);

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IGrpcPushConsumer>();

        var mappedOptions = Assert.IsType<GrpcPushConsumerOptions>(capturedOptions);
        Assert.Equal("original-consumer", mappedOptions.GroupName);
        Assert.Equal(59, mappedOptions.MaxConcurrency);
        Assert.Equal(61, mappedOptions.BatchSize);
        Assert.Equal(67, mappedOptions.MaxCachedMessages);
        Assert.Equal(71 * 1024, mappedOptions.MaxCachedMessageBytes);
        Assert.Equal(73, mappedOptions.MaxDeliveryAttempts);
        Assert.Equal(TimeSpan.FromSeconds(79), mappedOptions.InvisibleDuration);
        Assert.Equal(TimeSpan.FromMinutes(83), mappedOptions.ConsumeTimeout);
        Assert.Equal(TimeSpan.FromSeconds(89), mappedOptions.LongPollingTimeout);
        Assert.Equal(TimeSpan.FromSeconds(97), mappedOptions.RetryDelay);
    }

    [Fact]
    public async Task AddGrpcEventBus_SnapshotsProducerOptionsAtRegistration()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        GrpcProducerOptions? capturedOptions = null;
        GrpcEventBusProducerOptions? retainedOptions = null;
        services.PostConfigureAll<GrpcProducerOptions>(options => capturedOptions = options);
        services
            .AddRocketMQGrpc(ConfigureClient)
            .AddGrpcEventBus(configureProducer: options =>
            {
                retainedOptions = options;
                options.SendMsgTimeout = TimeSpan.FromSeconds(149);
                options.RetryTimesWhenSendFailed = 151;
                options.MaxMessageSize = 157 * 1024;
            });
        var wrapper = retainedOptions ?? throw new InvalidOperationException(
            "The producer options delegate was not invoked.");
        wrapper.SendMsgTimeout = TimeSpan.FromSeconds(163);
        wrapper.RetryTimesWhenSendFailed = 167;
        wrapper.MaxMessageSize = 173;

        await using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IGrpcProducer>();

        var mappedOptions = Assert.IsType<GrpcProducerOptions>(capturedOptions);
        Assert.Equal(TimeSpan.FromSeconds(149), mappedOptions.SendMsgTimeout);
        Assert.Equal(151, mappedOptions.RetryTimesWhenSendFailed);
        Assert.Equal(157 * 1024, mappedOptions.MaxMessageSize);
        Assert.Empty(mappedOptions.Topics);
        Assert.Null(mappedOptions.TransactionChecker);
        Assert.Equal(
            new GrpcProducerOptions().MaxConcurrentTransactionChecks,
            mappedOptions.MaxConcurrentTransactionChecks);
    }

    private static void ConfigureClient(GrpcClientOptions options) => options.Endpoint = "http://127.0.0.1:8081";
}
