using System.Collections.Concurrent;

namespace EventHorizon.RocketMQ.Remoting.EventBus.Tests.Support.Logging;

internal sealed class CollectingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<CollectedLogEntry> _entries = new();

    internal IReadOnlyList<CollectedLogEntry> Entries => _entries.ToArray();

    public ILogger CreateLogger(string categoryName) => new CollectingLogger(categoryName, _entries);

    public void Dispose()
    {
    }

    internal sealed record CollectedLogEntry(LogLevel Level, string CategoryName, string Message);

    private sealed class CollectingLogger(string categoryName, ConcurrentQueue<CollectedLogEntry> entries) : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            entries.Enqueue(new CollectedLogEntry(logLevel, categoryName, formatter(state, exception)));
        }

        private sealed class NullScope : IDisposable
        {
            internal static NullScope Instance { get; } = new();

            public void Dispose()
            {
            }
        }
    }
}
