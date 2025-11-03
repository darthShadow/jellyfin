using Jellyfin.Database.Benchmarks.Benchmarks;
using Jellyfin.Database.Benchmarks.Current;
using Jellyfin.Database.Benchmarks.Locking;
using Jellyfin.Database.Benchmarks.Proposed;

namespace Jellyfin.Database.Benchmarks;

/// <summary>
/// Benchmark runner comparing single-pool vs dual-pool architectures
/// across 3 locking behaviors and varying concurrency levels.
/// </summary>
public class Program
{
    private static readonly int[] ConcurrencyLevels = { 1, 5, 10, 20, 50 };
    private const int OperationsPerThread = 50;

    public static async Task Main(string[] args)
    {
        Console.WriteLine("================================================================================");
        Console.WriteLine("  Jellyfin Database Architecture Benchmark");
        Console.WriteLine("  Comparing: Single-Pool vs Dual-Pool");
        Console.WriteLine("  Locking:   NoLock, Pessimistic, Optimistic");
        Console.WriteLine("================================================================================\n");

        // Create test configurations (2 architectures × 3 locking behaviors = 6 configs)
        var configurations = CreateConfigurations();

        var allResults = new List<MetricsSnapshot>();

        // Test 1: Pure Read Workload
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  TEST 1: Pure Read Workload (100% reads)                                   │");
        Console.WriteLine("│  Expected: Dual-pool should show 2-8x improvement over single-pool         │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        foreach (var config in configurations)
        {
            Console.WriteLine($"Testing: {config.Name}");
            foreach (var concurrency in ConcurrencyLevels)
            {
                Console.Write($"  Concurrency {concurrency,3}... ");
                var result = await WorkloadBenchmark.RunAsync(config, WorkloadType.PureRead, concurrency, OperationsPerThread);
                Console.WriteLine($"{result.ThroughputOpsPerSec:F2} ops/sec (p95: {result.LatencyP95:F2}ms)");
                allResults.Add(result);
            }
            Console.WriteLine();
        }

        // Test 2: Pure Write Workload
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  TEST 2: Pure Write Workload (100% writes)                                 │");
        Console.WriteLine("│  Expected: Lock contention visible; pessimistic serializes, optimistic     │");
        Console.WriteLine("│            retries, NoLock may see SQLITE_BUSY errors                       │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        foreach (var config in configurations)
        {
            Console.WriteLine($"Testing: {config.Name}");
            foreach (var concurrency in ConcurrencyLevels)
            {
                Console.Write($"  Concurrency {concurrency,3}... ");
                var result = await WorkloadBenchmark.RunAsync(config, WorkloadType.PureWrite, concurrency, OperationsPerThread);
                Console.WriteLine($"{result.ThroughputOpsPerSec:F2} ops/sec (p95: {result.LatencyP95:F2}ms, errors: {result.ErrorCount})");
                allResults.Add(result);
            }
            Console.WriteLine();
        }

        // Test 3: Mixed Workload (90% reads, 10% writes)
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  TEST 3: Mixed Workload (90% reads, 10% writes)                            │");
        Console.WriteLine("│  Expected: Realistic Jellyfin usage; dual-pool should show significant     │");
        Console.WriteLine("│            improvement as reads don't block on writes                       │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        foreach (var config in configurations)
        {
            Console.WriteLine($"Testing: {config.Name}");
            foreach (var concurrency in ConcurrencyLevels)
            {
                Console.Write($"  Concurrency {concurrency,3}... ");
                var result = await WorkloadBenchmark.RunAsync(config, WorkloadType.Mixed, concurrency, OperationsPerThread);
                Console.WriteLine($"{result.ThroughputOpsPerSec:F2} ops/sec (p95: {result.LatencyP95:F2}ms, errors: {result.ErrorCount})");
                allResults.Add(result);
            }
            Console.WriteLine();
        }

        // Test 4: Lock Contention Stress Test
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  TEST 4: Lock Contention Stress Test                                       │");
        Console.WriteLine("│  All threads update THE SAME row simultaneously (forces real contention)   │");
        Console.WriteLine("│  Expected: NoLock shows SQLITE_BUSY errors, Optimistic shows retries,      │");
        Console.WriteLine("│            Pessimistic serializes cleanly                                   │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        // Use smaller concurrency levels and fewer operations for stress test
        var stressLevels = new[] { 5, 10, 20 };
        const int stressOpsPerThread = 20;

        foreach (var config in configurations)
        {
            Console.WriteLine($"Testing: {config.Name}");
            foreach (var concurrency in stressLevels)
            {
                Console.Write($"  Concurrency {concurrency,3}... ");
                var result = await WorkloadBenchmark.RunAsync(config, WorkloadType.Contention, concurrency, stressOpsPerThread);
                Console.WriteLine($"{result.ThroughputOpsPerSec:F2} ops/sec (p95: {result.LatencyP95:F2}ms, errors: {result.ErrorCount})");
                allResults.Add(result);
            }
            Console.WriteLine();
        }

        // Test 5: Data Integrity Problems (All Configurations)
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  TEST 5: Data Integrity Problems (All Configurations)                      │");
        Console.WriteLine("│  Part 1: Pool concurrency test (poolSize doesn't limit concurrency)        │");
        Console.WriteLine("│  Part 2: Concurrent updates (how each locking strategy handles conflicts)  │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        foreach (var config in configurations)
        {
            Console.WriteLine($"\n▶ Testing: {config.Name}");
            await DataIntegrityTest.RunAsync(config);
        }

        // Test 6: SerializedWriteScope Solution (Dual-Pool Only)
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  TEST 6: SerializedWriteScope Solution (Dual-Pool Only)                    │");
        Console.WriteLine("│  Same scenario as Test 5 Part 2, but with SerializedWriteScope             │");
        Console.WriteLine("│  Expected: Problems automatically fixed regardless of locking strategy     │");
        Console.WriteLine("│  Note: SerializedWriteScope only available in Dual-Pool architecture       │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        var dualPoolConfigs = configurations.Where(c => c.Architecture == ArchitectureType.DualPool).ToList();
        foreach (var config in dualPoolConfigs)
        {
            Console.WriteLine($"\n▶ Testing: {config.Name}");
            await SerializedWriteTest.RunAsync(config.ServiceProvider);
        }

        // Summary
        PrintComparisonSummary(allResults);

        Console.WriteLine("\n================================================================================");
        Console.WriteLine("  Benchmark Complete");
        Console.WriteLine("================================================================================\n");
    }

    private static List<BenchmarkConfiguration> CreateConfigurations()
    {
        var configs = new List<BenchmarkConfiguration>();
        var lockingBehaviorTypes = new (string Name, Func<IEntityFrameworkCoreLockingBehavior> Factory)[]
        {
            ("NoLock", () => new NoLockBehavior()),
            ("Pessimistic", () => new PessimisticLockBehavior()),
            ("Optimistic", () => new OptimisticLockBehavior())
        };

        var tempDir = Path.Combine(Path.GetTempPath(), "jellyfin-benchmark");
        Directory.CreateDirectory(tempDir);

        foreach (var (name, lockingFactory) in lockingBehaviorTypes)
        {
            // Single-pool configuration (create NEW instance)
            var singleLocking = lockingFactory();
            var singleDbPath = Path.Combine(tempDir, $"single-{name.ToLower()}.db");
            var singleProvider = SinglePoolSetup.CreateServiceProvider(singleDbPath, singleLocking, poolSize: 4);
            SinglePoolSetup.InitializeDatabaseAsync(singleProvider).GetAwaiter().GetResult();

            configs.Add(new BenchmarkConfiguration
            {
                Name = $"Single-Pool + {name}",
                Architecture = ArchitectureType.SinglePool,
                LockingBehavior = singleLocking,
                ServiceProvider = singleProvider
            });

            // Dual-pool configuration (create NEW instance)
            var dualLocking = lockingFactory();
            var dualDbPath = Path.Combine(tempDir, $"dual-{name.ToLower()}.db");
            var dualProvider = DualPoolSetup.CreateServiceProvider(dualDbPath, dualLocking, readerPoolSize: 8, writerPoolSize: 1);
            DualPoolSetup.InitializeDatabaseAsync(dualProvider).GetAwaiter().GetResult();

            configs.Add(new BenchmarkConfiguration
            {
                Name = $"Dual-Pool + {name}",
                Architecture = ArchitectureType.DualPool,
                LockingBehavior = dualLocking,
                ServiceProvider = dualProvider
            });
        }

        return configs;
    }

    private static void PrintComparisonSummary(List<MetricsSnapshot> results)
    {
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  COMPARISON SUMMARY                                                         │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        // Group by locking behavior and find improvement ratios
        var lockingTypes = new[] { "NoLock", "Pessimistic", "Optimistic" };

        foreach (var lockingType in lockingTypes)
        {
            Console.WriteLine($"\n{lockingType} Locking:");
            Console.WriteLine("─────────────────────────────────────────────────────────────────────────────");

            var singlePoolResults = results
                .Where(r => r.ConfigurationName.Contains($"Single-Pool + {lockingType}"))
                .OrderBy(r => r.ConcurrencyLevel)
                .ToList();

            var dualPoolResults = results
                .Where(r => r.ConfigurationName.Contains($"Dual-Pool + {lockingType}"))
                .OrderBy(r => r.ConcurrencyLevel)
                .ToList();

            Console.WriteLine($"{"Concurrency",12} | {"Single-Pool",15} | {"Dual-Pool",15} | {"Improvement",12}");
            Console.WriteLine(new string('─', 75));

            for (int i = 0; i < Math.Min(singlePoolResults.Count, dualPoolResults.Count); i++)
            {
                var single = singlePoolResults[i];
                var dual = dualPoolResults[i];

                if (single.ConcurrencyLevel == dual.ConcurrencyLevel)
                {
                    var improvement = dual.ThroughputOpsPerSec / single.ThroughputOpsPerSec;
                    var improvementStr = improvement >= 1.0
                        ? $"{improvement:F2}x faster"
                        : $"{(1 / improvement):F2}x slower";

                    Console.WriteLine(
                        $"{single.ConcurrencyLevel,12} | " +
                        $"{single.ThroughputOpsPerSec,12:F2} op/s | " +
                        $"{dual.ThroughputOpsPerSec,12:F2} op/s | " +
                        $"{improvementStr,12}");
                }
            }
        }

        // Error and Data Integrity Analysis
        Console.WriteLine("\n\nError Analysis:");
        Console.WriteLine("─────────────────────────────────────────────────────────────────────────────");

        var errorResults = results.Where(r => r.ErrorCount > 0).ToList();
        if (errorResults.Any())
        {
            foreach (var result in errorResults)
            {
                var errorRate = (double)result.ErrorCount / result.OperationCount * 100;
                Console.WriteLine($"{result.ConfigurationName,-30} | Concurrency {result.ConcurrencyLevel,2} | {result.ErrorCount,4} errors ({errorRate:F2}%)");
            }
        }
        else
        {
            Console.WriteLine("No errors detected in any configuration.");
        }

        // Data Integrity Analysis (for Contention tests)
        Console.WriteLine("\n\nData Integrity Analysis (Contention Tests Only):");
        Console.WriteLine("─────────────────────────────────────────────────────────────────────────────");

        var integrityResults = results
            .Where(r => r.ExpectedConcurrencyToken.HasValue)
            .OrderBy(r => r.ConfigurationName)
            .ThenBy(r => r.ConcurrencyLevel)
            .ToList();

        if (integrityResults.Any())
        {
            foreach (var result in integrityResults)
            {
                var expected = result.ExpectedConcurrencyToken!.Value;
                var actual = result.ActualConcurrencyToken!.Value;
                var dataLoss = expected - actual;
                var dataLossPercent = (double)dataLoss / expected * 100;

                var integrity = dataLoss == 0 ? "✅ OK" : $"❌ LOST {dataLoss} ({dataLossPercent:F1}%)";

                Console.Write($"{result.ConfigurationName,-30} | Concurrency {result.ConcurrencyLevel,2} | ");
                Console.Write($"Token: {actual}/{expected} | {integrity}");

                // Show OptimisticLocking retry stats
                if (result.OptimisticRetries.HasValue && result.OptimisticRetries.Value > 0)
                {
                    Console.Write($" | Retries: {result.OptimisticRetries} ({result.OptimisticConflicts} conflicts)");
                }

                Console.WriteLine();
            }
        }
        else
        {
            Console.WriteLine("No contention test results with data integrity tracking.");
        }
    }
}
