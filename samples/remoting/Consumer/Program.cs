using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Handlers;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Handlers.Orders;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.EventBus;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
var nameserver = builder.Configuration["RocketMQ:NamesrvAddr"] ?? "localhost:9876";
const string ordersRegistrationName = "orders";

// The EventBus consumer wrapper owns the group and delivery policy for this registration.
// The default true policy logs malformed payloads at Error, skips the handler, and acknowledges Success.
builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = nameserver)
    .AddRemotingEventBus(options =>
    {
        options.GroupName = "eventbus-remoting-sample";
        options.MaxConcurrency = 8;
        options.SkipDeserializationFailures = true;
    })
    // If every Handler in this assembly belongs to this registration, replace the individual calls below with:
    // .AddHandlersFromAssemblyOf<Program>();
    .AddHandler<OrderSubmittedHandler>()
    .AddHandler<OrderSubmittedAuditHandler>()
    .AddHandler<InventorySnapshotHandler>();
builder.Services
    .AddRocketMQRemoting(ordersRegistrationName, options => options.NamesrvAddr = nameserver)
    .AddRemotingEventBus(options =>
    {
        options.GroupName = "eventbus-remoting-orders-sample";
        options.MaxConcurrency = 8;
        // Set this to false to request ordinary retry instead of acknowledging malformed payloads.
        options.SkipDeserializationFailures = true;
    })
    .AddHandler<OrdersOrderSubmittedHandler>();

await builder.Build().RunAsync();
