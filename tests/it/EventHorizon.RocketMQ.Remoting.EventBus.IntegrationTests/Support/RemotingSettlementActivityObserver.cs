using System.Collections.Concurrent;
using System.Diagnostics;

namespace EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.Support;

internal sealed class RemotingSettlementActivityObserver : IDisposable
{
    private readonly ConcurrentQueue<SettlementObservation> _observations = new();
    private readonly SemaphoreSlim _changed = new(0);
    private readonly ActivityListener _listener;

    public RemotingSettlementActivityObserver()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == RemotingRocketMQInstrumentation.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = ObserveStoppedActivity
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public async Task WaitForDistinctMessagesAsync(
        string operationName,
        string topic,
        string consumerGroup,
        int count,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationName);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);

        var elapsed = Stopwatch.StartNew();
        while (true)
        {
            var matching = _observations
                .Where(observation =>
                    string.Equals(observation.OperationName, operationName, StringComparison.Ordinal) &&
                    string.Equals(observation.Topic, topic, StringComparison.Ordinal) &&
                    string.Equals(observation.ConsumerGroup, consumerGroup, StringComparison.Ordinal))
                .ToArray();
            var failed = matching.FirstOrDefault(static observation => observation.Failed);
            if (failed is not null)
            {
                throw new InvalidOperationException(
                    $"Remoting settlement '{operationName}' failed for {topic}/{consumerGroup}: " +
                    $"{failed.ErrorType ?? "unknown error"}.");
            }

            var distinctMessageCount = matching
                .Select(static observation => observation.MessageId)
                .Where(static messageId => !string.IsNullOrWhiteSpace(messageId))
                .Distinct(StringComparer.Ordinal)
                .Count();
            if (distinctMessageCount >= count)
            {
                return;
            }

            var remaining = timeout - elapsed.Elapsed;
            if (remaining <= TimeSpan.Zero ||
                !await _changed.WaitAsync(remaining, cancellationToken).ConfigureAwait(false))
            {
                throw new TimeoutException(
                    $"Observed {distinctMessageCount} of {count} distinct successful Remoting '{operationName}' " +
                    $"settlements for {topic}/{consumerGroup} within {timeout}.");
            }
        }
    }

    public void Dispose()
    {
        _listener.Dispose();
        _changed.Dispose();
    }

    private void ObserveStoppedActivity(Activity activity)
    {
        if (!string.Equals(
                activity.GetTagItem("messaging.operation.type") as string,
                "settle",
                StringComparison.Ordinal))
        {
            return;
        }

        _observations.Enqueue(new SettlementObservation(
            activity.GetTagItem("messaging.operation.name") as string ?? string.Empty,
            activity.GetTagItem("messaging.destination.name") as string ?? string.Empty,
            activity.GetTagItem("messaging.consumer.group.name") as string ?? string.Empty,
            activity.GetTagItem("messaging.message.id") as string,
            activity.Status == ActivityStatusCode.Error,
            activity.GetTagItem("error.type") as string));
        _changed.Release();
    }

    private sealed record SettlementObservation(
        string OperationName,
        string Topic,
        string ConsumerGroup,
        string? MessageId,
        bool Failed,
        string? ErrorType);
}
