using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Handlers;
using EventHorizon.RocketMQ.EventBus.Samples.Grpc.Consumer.Handlers.Orders;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.EventBus;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
var endpoint = builder.Configuration["RocketMQ:GrpcEndpoint"] ?? "http://localhost:8081";
const string ordersRegistrationName = "orders";

// The EventBus consumer wrapper owns the group and delivery policy for this registration.
// The default true policy logs malformed payloads at Error, skips the handler, and acknowledges Success.
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = endpoint)
    .AddGrpcEventBus(options =>
    {
        options.GroupName = "eventbus-grpc-sample";
        options.MaxConcurrency = 8;
        options.SkipDeserializationFailures = true;
    })
    // If every Handler in this assembly belongs to this registration, replace the individual calls below with:
    // .AddHandlersFromAssemblyOf<Program>();
    .AddHandler<OrderSubmittedHandler>()
    .AddHandler<OrderSubmittedAuditHandler>()
    .AddHandler<InventorySnapshotHandler>();
builder.Services
    .AddRocketMQGrpc(ordersRegistrationName, options => options.Endpoint = endpoint)
    .AddGrpcEventBus(options =>
    {
        options.GroupName = "eventbus-grpc-orders-sample";
        options.MaxConcurrency = 8;
        // Set this to false to request ordinary retry instead of acknowledging malformed payloads.
        options.SkipDeserializationFailures = true;
    })
    .AddHandler<OrdersOrderSubmittedHandler>();

await builder.Build().RunAsync();
