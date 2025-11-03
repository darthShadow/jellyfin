using System.Diagnostics;

namespace Jellyfin.Database.Benchmarks.Benchmarks;

/// <summary>
/// Captures performance metrics for a benchmark run.
/// </summary>
public class BenchmarkMetrics
{
    private readonly List<double> _latencies = new();
    private readonly Stopwatch _totalTimer = Stopwatch.StartNew();
    private int _successCount;
    private int _errorCount;

    public string ConfigurationName { get; init; } = string.Empty;
    public int ConcurrencyLevel { get; init; }
    public int OperationCount { get; init; }

    /// <summary>
    /// Gets the current success count (thread-safe).
    /// </summary>
    public int SuccessCount => _successCount;

    /// <summary>
    /// Gets the current error count (thread-safe).
    /// </summary>
    public int ErrorCount => _errorCount;

    // Data integrity tracking
    public uint? ExpectedConcurrencyToken { get; set; }
    public uint? ActualConcurrencyToken { get; set; }
    public int? OptimisticRetries { get; set; }
    public int? OptimisticConflicts { get; set; }

    /// <summary>
    /// Records a successful operation with its latency.
    /// </summary>
    public void RecordSuccess(TimeSpan latency)
    {
        lock (_latencies)
        {
            _latencies.Add(latency.TotalMilliseconds);
            _successCount++;
        }
    }

    /// <summary>
    /// Records a failed operation.
    /// </summary>
    public void RecordError()
    {
        Interlocked.Increment(ref _errorCount);
    }

    /// <summary>
    /// Gets the final metrics after all operations complete.
    /// </summary>
    public MetricsSnapshot GetSnapshot()
    {
        _totalTimer.Stop();

        lock (_latencies)
        {
            _latencies.Sort();

            return new MetricsSnapshot
            {
                ConfigurationName = ConfigurationName,
                ConcurrencyLevel = ConcurrencyLevel,
                OperationCount = OperationCount,
                SuccessCount = _successCount,
                ErrorCount = _errorCount,
                TotalDurationMs = _totalTimer.Elapsed.TotalMilliseconds,
                ThroughputOpsPerSec = _successCount / _totalTimer.Elapsed.TotalSeconds,
                LatencyP50 = GetPercentile(_latencies, 0.50),
                LatencyP95 = GetPercentile(_latencies, 0.95),
                LatencyP99 = GetPercentile(_latencies, 0.99),
                LatencyMin = _latencies.Count > 0 ? _latencies[0] : 0,
                LatencyMax = _latencies.Count > 0 ? _latencies[^1] : 0,
                LatencyAvg = _latencies.Count > 0 ? _latencies.Average() : 0,
                ExpectedConcurrencyToken = ExpectedConcurrencyToken,
                ActualConcurrencyToken = ActualConcurrencyToken,
                OptimisticRetries = OptimisticRetries,
                OptimisticConflicts = OptimisticConflicts
            };
        }
    }

    private static double GetPercentile(List<double> sortedValues, double percentile)
    {
        if (sortedValues.Count == 0)
            return 0;

        var index = (int)Math.Ceiling(percentile * sortedValues.Count) - 1;
        index = Math.Max(0, Math.Min(index, sortedValues.Count - 1));
        return sortedValues[index];
    }
}

/// <summary>
/// Immutable snapshot of benchmark metrics.
/// </summary>
public record MetricsSnapshot
{
    public string ConfigurationName { get; init; } = string.Empty;
    public int ConcurrencyLevel { get; init; }
    public int OperationCount { get; init; }
    public int SuccessCount { get; init; }
    public int ErrorCount { get; init; }
    public double TotalDurationMs { get; init; }
    public double ThroughputOpsPerSec { get; init; }
    public double LatencyP50 { get; init; }
    public double LatencyP95 { get; init; }
    public double LatencyP99 { get; init; }
    public double LatencyMin { get; init; }
    public double LatencyMax { get; init; }
    public double LatencyAvg { get; init; }

    // Data integrity tracking
    public uint? ExpectedConcurrencyToken { get; init; }
    public uint? ActualConcurrencyToken { get; init; }
    public int? OptimisticRetries { get; init; }
    public int? OptimisticConflicts { get; init; }

    public void PrintSummary()
    {
        Console.WriteLine($"\n  Configuration: {ConfigurationName}");
        Console.WriteLine($"  Concurrency:   {ConcurrencyLevel} threads");
        Console.WriteLine($"  Operations:    {OperationCount} total ({SuccessCount} success, {ErrorCount} errors)");
        Console.WriteLine($"  Duration:      {TotalDurationMs:F2} ms");
        Console.WriteLine($"  Throughput:    {ThroughputOpsPerSec:F2} ops/sec");
        Console.WriteLine($"  Latency (ms):  min={LatencyMin:F2}, avg={LatencyAvg:F2}, p50={LatencyP50:F2}, p95={LatencyP95:F2}, p99={LatencyP99:F2}, max={LatencyMax:F2}");
    }
}
