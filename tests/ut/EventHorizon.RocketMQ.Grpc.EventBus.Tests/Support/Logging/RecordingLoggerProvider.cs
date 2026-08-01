namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Support.Logging;

internal sealed class RecordingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<RecordedLogEntry> _entries = new();

    internal IReadOnlyList<RecordedLogEntry> Entries => _entries.ToArray();

    public ILogger CreateLogger(string categoryName) => new RecordingLogger(categoryName, _entries);

    public void Dispose()
    {
    }

    private sealed class RecordingLogger(string categoryName, ConcurrentQueue<RecordedLogEntry> entries) : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => EmptyScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            entries.Enqueue(new RecordedLogEntry(categoryName, logLevel, formatter(state, exception), exception));
        }
    }

    private sealed class EmptyScope : IDisposable
    {
        internal static EmptyScope Instance { get; } = new();

        public void Dispose()
        {
        }
    }
}

internal sealed record RecordedLogEntry(
    string Category,
    LogLevel LogLevel,
    string Message,
    Exception? Exception);
