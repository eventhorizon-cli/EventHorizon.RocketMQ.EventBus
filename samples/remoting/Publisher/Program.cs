using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Events;
using EventHorizon.RocketMQ.EventBus.Exceptions;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Publisher.Events;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Publisher.Events.Orders;
using EventHorizon.RocketMQ.EventBus.Samples.Remoting.Publisher.Requests;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.EventBus;
using Microsoft.OpenApi;

const string OrdersRegistrationName = "orders";

var builder = WebApplication.CreateBuilder(args);
var nameserver = builder.Configuration["RocketMQ:NamesrvAddr"] ?? "localhost:9876";
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options => options.SwaggerDoc("v1", new OpenApiInfo
{
    Title = "RocketMQ Remoting EventBus Publisher",
    Version = "v1",
}));

builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = nameserver)
    .AddRemotingEventBus(configureProducer: options =>
    {
        options.GroupName = "eventbus-remoting-sample-publisher";
    });
builder.Services
    .AddRocketMQRemoting(OrdersRegistrationName, options => options.NamesrvAddr = nameserver)
    .AddRemotingEventBus(configureProducer: options => options.GroupName = "eventbus-remoting-orders-publisher");

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/events/orders", PublishDefaultOrderAsync)
    .WithName("PublishRemotingOrder")
    .WithSummary("Publishes a tagged order through the default Remoting EventBus registration.")
    .Produces(StatusCodes.Status202Accepted)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
app.MapPost("/events/inventory-snapshots", PublishInventorySnapshotAsync)
    .WithName("PublishRemotingInventorySnapshot")
    .WithSummary("Publishes an untagged inventory snapshot through the default Remoting EventBus registration.")
    .Produces(StatusCodes.Status202Accepted)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
app.MapPost("/clients/orders/events", PublishNamedOrderAsync)
    .WithName("PublishNamedRemotingOrder")
    .WithSummary("Publishes a tagged order through the keyed orders Remoting EventBus registration.")
    .Produces(StatusCodes.Status202Accepted)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

await app.RunAsync();

static Task<IResult> PublishDefaultOrderAsync(
    OrderSubmissionRequest? request,
    IEventBus eventBus,
    CancellationToken cancellationToken) =>
    PublishOrderAsync(
        request,
        eventBus,
        static (orderId, total) => new OrderSubmittedIntegrationEvent { OrderId = orderId, Total = total },
        cancellationToken);

static Task<IResult> PublishNamedOrderAsync(
    OrderSubmissionRequest? request,
    [FromKeyedServices(OrdersRegistrationName)] IEventBus eventBus,
    CancellationToken cancellationToken) =>
    PublishOrderAsync(
        request,
        eventBus,
        static (orderId, total) => new OrdersOrderSubmittedIntegrationEvent { OrderId = orderId, Total = total },
        cancellationToken);

static async Task<IResult> PublishOrderAsync(
    OrderSubmissionRequest? request,
    IEventBus eventBus,
    Func<Guid, decimal, IntegrationEvent> createEvent,
    CancellationToken cancellationToken)
{
    if (request is null || request.OrderId == Guid.Empty || request.Total < 0)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["request"] = ["OrderId must be non-empty and Total must be non-negative."],
        });
    }

    try
    {
        await eventBus.PublishAsync(
            createEvent(request.OrderId, request.Total),
            cancellationToken).ConfigureAwait(false);
        return Results.Accepted();
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (EventBusPublishException)
    {
        return Results.Problem(
            title: "RocketMQ EventBus publish failed.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}

static async Task<IResult> PublishInventorySnapshotAsync(
    InventorySnapshotRequest? request,
    IEventBus eventBus,
    CancellationToken cancellationToken)
{
    if (string.IsNullOrWhiteSpace(request?.Warehouse))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["warehouse"] = ["Warehouse is required."],
        });
    }

    try
    {
        await eventBus.PublishAsync(
            new InventorySnapshotIntegrationEvent { Warehouse = request.Warehouse, RecordedAt = DateTimeOffset.UtcNow },
            cancellationToken).ConfigureAwait(false);
        return Results.Accepted();
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (EventBusPublishException)
    {
        return Results.Problem(
            title: "RocketMQ EventBus publish failed.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}
