namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Registration;

public sealed class RemotingEventBusBuilderExtensionsTests
{
    [Fact]
    public void AddRemotingEventBus_WithoutAProducer_DoesNotRegisterIEventBus()
    {
        var services = new ServiceCollection();

        var eventBusBuilder = services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus();

        using var provider = services.BuildServiceProvider();

        Assert.Null(eventBusBuilder.RegistrationName);
        Assert.Null(provider.GetService<IEventBus>());
        Assert.DoesNotContain(services, static descriptor => descriptor.ServiceType == typeof(IRemotingProducer));
    }

    [Fact]
    public async Task AddRemotingEventBus_WithAProducer_RegistersAnUnkeyedIEventBus()
    {
        var services = new ServiceCollection();

        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(
                configureProducer: options => options.GroupName = "eventbus-publisher");

        await using var provider = services.BuildServiceProvider();

        Assert.IsAssignableFrom<IEventBus>(provider.GetRequiredService<IEventBus>());
        Assert.Contains(services, static descriptor => descriptor.ServiceType == typeof(IRemotingProducer));
    }

    [Fact]
    public async Task AddRemotingEventBus_WithANamedProducer_RegistersOnlyTheMatchingKeyedIEventBus()
    {
        var services = new ServiceCollection();

        services
            .AddRocketMQRemoting("orders", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus(
                configureProducer: options => options.GroupName = "orders-publisher");

        await using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<IEventBus>());
        Assert.IsAssignableFrom<IEventBus>(provider.GetRequiredKeyedService<IEventBus>("orders"));
    }

    [Fact]
    public void AddRemotingEventBus_UsesTheMainClientRegistrationName()
    {
        var services = new ServiceCollection();

        var eventBusBuilder = services
            .AddRocketMQRemoting("orders", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingEventBus();

        Assert.Equal("orders", eventBusBuilder.RegistrationName);
    }
}
