using System.Reflection;
using System.Reflection.Emit;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.EventBus.Tests.Registration;

public sealed class EventBusRegistrationTests
{
    [Fact]
    public void Create_RejectsDuplicateDefaultAndOrdinalEqualNamedIdentities()
    {
        var services = new ServiceCollection();

        EventBusRegistration.Create(services, null);
        EventBusRegistration.Create(services, "orders");
        EventBusRegistration.Create(services, "Orders");

        Assert.Throws<InvalidOperationException>(() => EventBusRegistration.Create(services, null));
        Assert.Throws<InvalidOperationException>(() => EventBusRegistration.Create(services, "orders"));
    }

    [Fact]
    public void AddHandler_RegistersRoutesInDirectCallOrderAndBuildsDeterministicSubscriptions()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder
            .AddHandler<SubmittedFirstHandler>()
            .AddHandler<SubmittedSecondHandler>()
            .AddHandler<CancelledHandler>()
            .AddHandler<AccountCreatedHandler>();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true,
        });
        var routePlan = registration.GetRequiredRoutePlan(provider);

        Assert.Equal(4, routePlan.HandlerCount);
        Assert.Collection(
            routePlan.Subscriptions,
            subscription =>
            {
                Assert.Equal("accounts", subscription.Topic);
                Assert.Equal("created", subscription.FilterExpression);
            },
            subscription =>
            {
                Assert.Equal("orders", subscription.Topic);
                Assert.Equal("cancelled || submitted", subscription.FilterExpression);
            });
        Assert.True(routePlan.TryGetRoute("orders", "submitted", out var route));
        Assert.Equal(typeof(OrderSubmittedEvent), route.IntegrationEventType);
        Assert.Equal(
            [typeof(SubmittedFirstHandler), typeof(SubmittedSecondHandler)],
            route.Handlers.Select(static handler => handler.HandlerType));
    }

    [Fact]
    public void RoutePlan_UsesExactOrdinalKeysWithoutNormalizingMetadata()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder
            .AddHandler<SubmittedFirstHandler>()
            .AddHandler<CaseVariantTagHandler>()
            .AddHandler<ExactWhitespaceRouteHandler>();

        using var provider = services.BuildServiceProvider();
        var routePlan = registration.GetRequiredRoutePlan(provider);

        Assert.True(routePlan.TryGetRoute("orders", "submitted", out _));
        Assert.True(routePlan.TryGetRoute("orders", "Submitted", out _));
        Assert.False(routePlan.TryGetRoute("orders", "SUBMITTED", out _));
        Assert.True(routePlan.TryGetRoute(" orders ", " submitted ", out _));
        Assert.False(routePlan.TryGetRoute("orders", " submitted ", out _));
        Assert.Contains(
            routePlan.Subscriptions,
            static subscription =>
                subscription.Topic == "orders" && subscription.FilterExpression == "Submitted || submitted");
        Assert.Contains(
            routePlan.Subscriptions,
            static subscription =>
                subscription.Topic == " orders " && subscription.FilterExpression == " submitted ");
    }

    [Fact]
    public void RoutePlan_UsesAnAllTagSubscriptionWhenATopicContainsAnUntaggedRoute()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder
            .AddHandler<SubmittedFirstHandler>()
            .AddHandler<UntaggedOrderHandler>();

        using var provider = services.BuildServiceProvider();
        var routePlan = registration.GetRequiredRoutePlan(provider);

        var subscription = Assert.Single(routePlan.Subscriptions);
        Assert.Equal("orders", subscription.Topic);
        Assert.Equal("*", subscription.FilterExpression);
        Assert.True(routePlan.TryGetRoute("orders", null, out var untaggedRoute));
        Assert.Equal(typeof(UntaggedOrderEvent), untaggedRoute.IntegrationEventType);
        Assert.True(routePlan.TryGetRoute("orders", "submitted", out var taggedRoute));
        Assert.Equal(typeof(OrderSubmittedEvent), taggedRoute.IntegrationEventType);
    }

    [Fact]
    public void AddHandler_RegistersEveryClosedInterfaceImplementedByOneHandler()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder.AddHandler<MultiEventHandler>();

        using var provider = services.BuildServiceProvider();
        var routePlan = registration.GetRequiredRoutePlan(provider);

        Assert.Equal(2, routePlan.HandlerCount);
        Assert.True(routePlan.TryGetRoute("orders", "submitted", out var submittedRoute));
        Assert.True(routePlan.TryGetRoute("orders", "cancelled", out var cancelledRoute));
        Assert.Equal(typeof(MultiEventHandler), submittedRoute.Handlers.Single().HandlerType);
        Assert.Equal(typeof(MultiEventHandler), cancelledRoute.Handlers.Single().HandlerType);
    }

    [Fact]
    public void AddHandler_IsIdempotentForTheSameLifetimeAndRejectsALifetimeConflict()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder.AddHandler<SubmittedFirstHandler>(ServiceLifetime.Scoped);
        registration.Builder.AddHandler<SubmittedFirstHandler>(ServiceLifetime.Scoped);

        Assert.Throws<InvalidOperationException>(() =>
            registration.Builder.AddHandler<SubmittedFirstHandler>(ServiceLifetime.Singleton));

        using var provider = services.BuildServiceProvider();
        Assert.Equal(1, registration.GetRequiredRoutePlan(provider).HandlerCount);
    }

    [Fact]
    public void AddHandler_RejectsTheSameHandlerAcrossDefaultAndNamedRegistrations()
    {
        var services = new ServiceCollection();
        var defaultRegistration = EventBusRegistration.Create(services, null);
        var namedRegistration = EventBusRegistration.Create(services, "orders");
        defaultRegistration.Builder.AddHandler<SubmittedFirstHandler>();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            namedRegistration.Builder.AddHandler<SubmittedFirstHandler>());

        Assert.Contains(typeof(SubmittedFirstHandler).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains("<default>", exception.Message, StringComparison.Ordinal);
        Assert.Contains("orders", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddHandlersFromAssembly_RejectsTheSameHandlersAcrossNamedRegistrations()
    {
        var services = new ServiceCollection();
        var ordersRegistration = EventBusRegistration.Create(services, "orders");
        var auditRegistration = EventBusRegistration.Create(services, "audit");
        var assembly = CreateScanningAssembly();
        ordersRegistration.Builder.AddHandlersFromAssembly(assembly);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            auditRegistration.Builder.AddHandlersFromAssembly(assembly));

        Assert.Contains("Scan.AlphaHandler", exception.Message, StringComparison.Ordinal);
        Assert.Contains("orders", exception.Message, StringComparison.Ordinal);
        Assert.Contains("audit", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddHandler_RejectsAmbiguousRoutes()
    {
        var (_, registration) = CreateRegistration();
        registration.Builder.AddHandler<SubmittedFirstHandler>();

        Assert.Throws<InvalidOperationException>(() => registration.Builder.AddHandler<AmbiguousRouteHandler>());
    }

    [Fact]
    public void AddHandler_RejectsEventsWithoutAPublicParameterlessConstructor()
    {
        var (_, registration) = CreateRegistration();

        Assert.Throws<InvalidOperationException>(() => registration.Builder.AddHandler<NoDefaultConstructorHandler>());
    }

    [Fact]
    public void AddHandler_RejectsEventsWhoseRouteConstructorThrows()
    {
        var (_, registration) = CreateRegistration();

        Assert.Throws<InvalidOperationException>(() => registration.Builder.AddHandler<ThrowingConstructorHandler>());
    }

    [Fact]
    public void AddHandler_RejectsProcessDependentRouteConstructors()
    {
        UnstableRouteEvent.Reset();
        var (_, registration) = CreateRegistration();

        Assert.Throws<InvalidOperationException>(() => registration.Builder.AddHandler<UnstableRouteHandler>());
    }

    [Fact]
    public void AddHandler_CreatesTheConsumerOnlyOnceAfterTheFirstHandler()
    {
        var consumerRegistrations = 0;
        var (_, registration) = CreateRegistration(ensureConsumer: _ => consumerRegistrations++);

        registration.Builder.AddHandler<SubmittedFirstHandler>();
        registration.Builder.AddHandler<SubmittedSecondHandler>();

        Assert.Equal(1, consumerRegistrations);
    }

    [Fact]
    public async Task AddHandler_RegistersAScopedAccessorForTheOwningConsumerAnchor()
    {
        var (services, registration) = CreateRegistration("orders");
        registration.Builder.AddHandler<SubmittedFirstHandler>();

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true,
        });
        await using var firstScope = provider.CreateAsyncScope();
        await using var secondScope = provider.CreateAsyncScope();

        var first = firstScope.ServiceProvider.GetRequiredService<EventBusRegistrationAccessor<SubmittedFirstHandler>>();
        var sameScope = firstScope.ServiceProvider.GetRequiredService<EventBusRegistrationAccessor<SubmittedFirstHandler>>();
        var second = secondScope.ServiceProvider.GetRequiredService<EventBusRegistrationAccessor<SubmittedFirstHandler>>();

        Assert.Same(first, sameScope);
        Assert.NotSame(first, second);
        Assert.Equal("orders", first.RegistrationName);
        Assert.Same(registration.GetRequiredDispatcher(firstScope.ServiceProvider), first.Dispatcher);
        Assert.Same(registration.GetRequiredRoutePlan(firstScope.ServiceProvider), first.RoutePlan);
        Assert.Same(registration.GetRequiredSerializer(firstScope.ServiceProvider), first.Serializer);
    }

    [Fact]
    public void AddHandlersFromAssembly_SortsDiscoveredHandlersByFullName()
    {
        var (services, registration) = CreateRegistration();
        var assembly = CreateScanningAssembly();

        registration.Builder.AddHandlersFromAssembly(assembly);

        using var provider = services.BuildServiceProvider();
        var routePlan = registration.GetRequiredRoutePlan(provider);
        Assert.True(routePlan.TryGetRoute("scan", "created", out var route));
        Assert.Equal(
            ["Scan.AlphaHandler", "Scan.ZuluHandler"],
            route.Handlers.Select(static handler => handler.HandlerType.FullName));
    }

    [Fact]
    public void BuiltProvider_UsesItsImmutableRegistrationSnapshot()
    {
        var (services, registration) = CreateRegistration();
        registration.Builder.AddHandler<SnapshotFirstHandler>();

        using var provider = services.BuildServiceProvider();
        registration.Builder.AddHandler<SnapshotSecondHandler>();

        var routePlan = registration.GetRequiredRoutePlan(provider);
        Assert.Single(routePlan.Subscriptions);
        Assert.True(routePlan.TryGetRoute("snapshot", "first", out _));
        Assert.False(routePlan.TryGetRoute("snapshot", "second", out _));
    }

    [Fact]
    public void Registrations_IsolateHandlerLifetimeAndSerializerInstances()
    {
        var services = new ServiceCollection();
        var first = EventBusRegistration.Create(services, "first");
        var second = EventBusRegistration.Create(services, "second");
        first.Builder.AddHandler<SubmittedFirstHandler>(ServiceLifetime.Scoped).UseSerializer<AlternateSerializer>();
        second.Builder.AddHandler<SubmittedSecondHandler>(ServiceLifetime.Singleton).UseSerializer<AlternateSerializer>();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var firstSerializer = first.GetRequiredSerializer(provider);
        var secondSerializer = second.GetRequiredSerializer(provider);

        Assert.IsType<AlternateSerializer>(firstSerializer);
        Assert.IsType<AlternateSerializer>(secondSerializer);
        Assert.NotSame(firstSerializer, secondSerializer);
        Assert.Equal(ServiceLifetime.Scoped, first.GetRequiredRoutePlan(provider).TryGetRoute("orders", "submitted", out var firstRoute)
            ? firstRoute.Handlers.Single().Lifetime
            : throw new Xunit.Sdk.XunitException("Expected first route."));
        Assert.Equal(ServiceLifetime.Singleton, second.GetRequiredRoutePlan(provider).TryGetRoute("orders", "submitted", out var secondRoute)
            ? secondRoute.Handlers.Single().Lifetime
            : throw new Xunit.Sdk.XunitException("Expected second route."));
    }

    [Fact]
    public void ConfigureLogging_DefaultsToEnabledPayloadsAndIsolatesNamedProviderSnapshots()
    {
        var services = new ServiceCollection();
        var orders = EventBusRegistration.Create(services, "orders");
        var audit = EventBusRegistration.Create(services, "audit");
        orders.Builder.ConfigureLogging(options => options.IncludePayload = false);

        using var firstProvider = services.BuildServiceProvider();

        Assert.False(orders.GetRequiredLoggingSettings(firstProvider).IncludePayload);
        Assert.True(orders.GetRequiredLoggingSettings(firstProvider).Enabled);
        Assert.True(audit.GetRequiredLoggingSettings(firstProvider).IncludePayload);
        Assert.True(audit.GetRequiredLoggingSettings(firstProvider).Enabled);

        orders.Builder.ConfigureLogging(options => options.IncludePayload = true);
        using var secondProvider = services.BuildServiceProvider();

        Assert.False(orders.GetRequiredLoggingSettings(firstProvider).IncludePayload);
        Assert.True(orders.GetRequiredLoggingSettings(secondProvider).IncludePayload);
    }

    private static (ServiceCollection Services, EventBusRegistration Registration) CreateRegistration(
        string? registrationName = null,
        Action<EventBusRegistration>? ensureConsumer = null)
    {
        var services = new ServiceCollection();
        return (services, EventBusRegistration.Create(services, registrationName, ensureConsumer));
    }

    private static Assembly CreateScanningAssembly()
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName($"EventBusScan{Guid.NewGuid():N}"),
            AssemblyBuilderAccess.Run);
        var module = assembly.DefineDynamicModule("Main");
        var integrationEventType = CreateIntegrationEvent(module);
        CreateHandler(module, "Scan.ZuluHandler", integrationEventType);
        CreateHandler(module, "Scan.AlphaHandler", integrationEventType);
        return assembly;
    }

    private static Type CreateIntegrationEvent(ModuleBuilder module)
    {
        var typeBuilder = module.DefineType(
            "Scan.CreatedEvent",
            TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed,
            typeof(IntegrationEvent));
        var constructor = typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
        var constructorIl = constructor.GetILGenerator();
        var baseConstructor = typeof(IntegrationEvent).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [typeof(string), typeof(string)],
            modifiers: null)!;
        constructorIl.Emit(OpCodes.Ldarg_0);
        constructorIl.Emit(OpCodes.Ldstr, "scan");
        constructorIl.Emit(OpCodes.Ldstr, "created");
        constructorIl.Emit(OpCodes.Call, baseConstructor);
        constructorIl.Emit(OpCodes.Ret);
        return typeBuilder.CreateType()!;
    }

    private static void CreateHandler(ModuleBuilder module, string name, Type integrationEventType)
    {
        var handlerInterface = typeof(IIntegrationEventBusHandler<>).MakeGenericType(integrationEventType);
        var typeBuilder = module.DefineType(name, TypeAttributes.Public | TypeAttributes.Class | TypeAttributes.Sealed);
        typeBuilder.AddInterfaceImplementation(handlerInterface);
        typeBuilder.DefineDefaultConstructor(MethodAttributes.Public);
        var method = typeBuilder.DefineMethod(
            nameof(IIntegrationEventBusHandler<IntegrationEvent>.HandleAsync),
            MethodAttributes.Public | MethodAttributes.Virtual,
            typeof(Task),
            [integrationEventType, typeof(CancellationToken)]);
        var methodIl = method.GetILGenerator();
        methodIl.Emit(OpCodes.Call, typeof(Task).GetProperty(nameof(Task.CompletedTask))!.GetMethod!);
        methodIl.Emit(OpCodes.Ret);
        typeBuilder.DefineMethodOverride(method, handlerInterface.GetMethod(nameof(IIntegrationEventBusHandler<IntegrationEvent>.HandleAsync))!);
        typeBuilder.CreateType();
    }
}
