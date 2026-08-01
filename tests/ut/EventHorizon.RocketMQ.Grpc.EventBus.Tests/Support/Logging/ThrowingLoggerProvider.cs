namespace EventHorizon.RocketMQ.Grpc.EventBus.Tests.Support.Logging;

internal sealed class ThrowingLoggerProvider : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => ThrowingLogger.Instance;

    public void Dispose()
    {
    }

    private sealed class ThrowingLogger : ILogger
    {
        internal static ThrowingLogger Instance { get; } = new();

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => EmptyScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            throw new InvalidOperationException("Expected logging failure.");

        private sealed class EmptyScope : IDisposable
        {
            internal static EmptyScope Instance { get; } = new();

            public void Dispose()
            {
            }
        }
    }
}
