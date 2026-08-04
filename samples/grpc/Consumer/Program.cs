using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Handlers;
using EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Handlers.Orders;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.EventBus;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
var endpoint = builder.Configuration["RocketMQ:GrpcEndpoint"] ?? "http://localhost:8081";
const string ordersRegistrationName = "orders";

builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = endpoint)
    .AddGrpcEventBus(options =>
    {
        options.GroupName = "eventbus-grpc-sample";
        options.MaxConcurrency = 8;
    })
    .AddHandler<OrderSubmittedHandler>()
    .AddHandler<OrderSubmittedAuditHandler>()
    .AddHandler<InventorySnapshotHandler>();
builder.Services
    .AddRocketMQGrpc(ordersRegistrationName, options => options.Endpoint = endpoint)
    .AddGrpcEventBus(options =>
    {
        options.GroupName = "eventbus-grpc-orders-sample";
        options.MaxConcurrency = 8;
    })
    .AddHandler<OrdersOrderSubmittedHandler>();

await builder.Build().RunAsync();
