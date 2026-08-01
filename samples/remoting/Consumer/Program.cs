using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Handlers;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Consumer.Handlers.Orders;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.EventBus;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
var nameserver = builder.Configuration["RocketMQ:NamesrvAddr"] ?? "localhost:9876";
const string OrdersRegistrationName = "orders";

builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = nameserver)
    .AddRemotingEventBus(options =>
    {
        options.GroupName = "eventbus-remoting-sample";
        options.MaxConcurrency = 8;
    })
    .AddHandler<OrderSubmittedHandler>()
    .AddHandler<OrderSubmittedAuditHandler>()
    .AddHandler<InventorySnapshotHandler>();
builder.Services
    .AddRocketMQRemoting(OrdersRegistrationName, options => options.NamesrvAddr = nameserver)
    .AddRemotingEventBus(options =>
    {
        options.GroupName = "eventbus-remoting-orders-sample";
        options.MaxConcurrency = 8;
    })
    .AddHandler<OrdersOrderSubmittedHandler>();

await builder.Build().RunAsync();
