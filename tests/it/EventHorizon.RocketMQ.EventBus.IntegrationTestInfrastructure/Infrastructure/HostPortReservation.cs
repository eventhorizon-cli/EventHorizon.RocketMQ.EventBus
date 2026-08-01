using System.Net;
using System.Net.Sockets;

namespace EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure.Infrastructure;

/// <summary>
/// Tracks dynamically selected host ports for the lifetime of a fixture in this process.
/// </summary>
/// <remarks>
/// The operating system listener used to probe a port is released before Docker binds it. This helper prevents two
/// fixtures in the same test process from selecting the same candidate; it cannot eliminate a process-external race.
/// </remarks>
internal sealed class HostPortReservation : IDisposable
{
    private static readonly object SyncRoot = new();
    private static readonly HashSet<int> ReservedPorts = [];
    private readonly IReadOnlyList<int> _ports;
    private int _disposed;

    private HostPortReservation(IReadOnlyList<int> ports)
    {
        _ports = ports;
    }

    internal int this[int index] => _ports[index];

    internal static HostPortReservation Reserve(int count)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(count, 0);

        lock (SyncRoot)
        {
            var ports = new List<int>(count);
            while (ports.Count < count)
            {
                var candidate = GetAvailablePort();
                if (!ReservedPorts.Add(candidate))
                {
                    continue;
                }

                ports.Add(candidate);
            }

            return new HostPortReservation(ports);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        lock (SyncRoot)
        {
            foreach (var port in _ports)
            {
                ReservedPorts.Remove(port);
            }
        }
    }

    private static int GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
