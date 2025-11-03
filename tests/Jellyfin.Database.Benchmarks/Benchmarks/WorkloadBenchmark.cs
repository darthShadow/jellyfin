using System.Diagnostics;
using Jellyfin.Database.Benchmarks.Current;
using Jellyfin.Database.Benchmarks.Entities;
using Jellyfin.Database.Benchmarks.Locking;
using Jellyfin.Database.Benchmarks.Proposed;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Jellyfin.Database.Benchmarks.Benchmarks;

/// <summary>
/// Workload types for benchmarking.
/// </summary>
public enum WorkloadType
{
    /// <summary>100% read operations.</summary>
    PureRead,

    /// <summary>100% write operations.</summary>
    PureWrite,

    /// <summary>90% reads, 10% writes (realistic Jellyfin usage).</summary>
    Mixed,

    /// <summary>All threads update the same row (forces lock contention).</summary>
    Contention
}

/// <summary>
/// Unified benchmark runner for all workload types.
/// </summary>
public static class WorkloadBenchmark
{
    private const double MixedWritePercentage = 0.10;

    public static async Task<MetricsSnapshot> RunAsync(
        BenchmarkConfiguration config,
        WorkloadType workloadType,
        int concurrencyLevel,
        int operationsPerThread)
    {
        var metrics = new BenchmarkMetrics
        {
            ConfigurationName = config.Name,
            ConcurrencyLevel = concurrencyLevel,
            OperationCount = concurrencyLevel * operationsPerThread
        };

        // Seed appropriate data for workload
        await SeedDataAsync(config, workloadType);

        // Reset OptimisticLocking counters before test
        if (config.LockingBehavior is OptimisticLockBehavior optimistic)
        {
            optimistic.ResetCounters();
        }

        // Run workload
        var tasks = Enumerable.Range(0, concurrencyLevel)
            .Select(threadId => RunThreadAsync(config, workloadType, threadId, operationsPerThread, metrics))
            .ToArray();

        await Task.WhenAll(tasks);

        // Validate data integrity for contention tests
        if (workloadType == WorkloadType.Contention)
        {
            await ValidateContentionDataIntegrity(config, metrics);
        }

        return metrics.GetSnapshot();
    }

    private static async Task SeedDataAsync(BenchmarkConfiguration config, WorkloadType workloadType)
    {
        if (config.Architecture == ArchitectureType.SinglePool)
        {
            var factory = config.ServiceProvider.GetRequiredService<IDbContextFactory<SinglePoolDbContext>>();
            await using var context = await factory.CreateDbContextAsync();
            await SeedDataInternalAsync(context, workloadType, config);
        }
        else
        {
            var manager = config.ServiceProvider.GetRequiredService<IDbContextManager>();
            await using var scope = await manager.CreateWriteContextAsync();
            await SeedDataInternalAsync(scope.Context, workloadType, config);
        }
    }

    private static async Task SeedDataInternalAsync(DbContext context, WorkloadType workloadType, BenchmarkConfiguration config)
    {
        var movies = context.Set<Movie>();
        var users = context.Set<User>();

        movies.RemoveRange(movies);
        users.RemoveRange(users);
        await SaveChangesAsync(config, context);

        switch (workloadType)
        {
            case WorkloadType.PureRead:
                // Large dataset for read benchmarks
                await SeedMoviesAsync(context, 1000);
                await SeedUsersAsync(context, 100);
                break;

            case WorkloadType.PureWrite:
                await SeedUsersAsync(context, 50);
                break;

            case WorkloadType.Mixed:
                await SeedMoviesAsync(context, 500);
                await SeedUsersAsync(context, 50);
                break;

            case WorkloadType.Contention:
                // Single hotspot user for contention test
                users.Add(new User
                {
                    Id = 999,
                    Username = "hotspot_user",
                    Email = "hotspot@test.com",
                    LastLogin = DateTime.UtcNow
                });
                break;
        }

        await SaveChangesAsync(config, context);
    }

    private static Task SeedMoviesAsync(DbContext context, int count)
    {
        var movies = Enumerable.Range(1, count)
            .Select(i => new Movie
            {
                Title = $"Movie {i}",
                Year = 2000 + (i % 24),
                Rating = 5.0 + (i % 50) / 10.0,
                ViewCount = i * 10
            })
            .ToList();
        context.Set<Movie>().AddRange(movies);
        return Task.CompletedTask;
    }

    private static Task SeedUsersAsync(DbContext context, int count)
    {
        var users = Enumerable.Range(1, count)
            .Select(i => new User
            {
                Username = $"user{i}",
                Email = $"user{i}@test.com",
                LastLogin = DateTime.UtcNow.AddDays(-i)
            })
            .ToList();
        context.Set<User>().AddRange(users);
        return Task.CompletedTask;
    }

    private static async Task RunThreadAsync(
        BenchmarkConfiguration config,
        WorkloadType workloadType,
        int threadId,
        int operationCount,
        BenchmarkMetrics metrics)
    {
        var random = new Random(threadId);

        for (int i = 0; i < operationCount; i++)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                switch (workloadType)
                {
                    case WorkloadType.PureRead:
                        await PerformReadAsync(config);
                        break;

                    case WorkloadType.PureWrite:
                        await PerformWriteAsync(config, threadId);
                        break;

                    case WorkloadType.Mixed:
                        if (random.NextDouble() < MixedWritePercentage)
                        {
                            await PerformWriteAsync(config, threadId);
                        }
                        else
                        {
                            await PerformReadAsync(config);
                        }
                        break;

                    case WorkloadType.Contention:
                        await PerformContentionWriteAsync(config, threadId, i);
                        break;
                }

                sw.Stop();
                metrics.RecordSuccess(sw.Elapsed);
            }
            catch (Exception)
            {
                sw.Stop();
                metrics.RecordError();
            }
        }
    }

    private static async Task PerformReadAsync(BenchmarkConfiguration config)
    {
        await using var context = await CreateReadContextAsync(config);

        var movies = await context.Set<Movie>()
            .Where(m => m.Year >= 2020)
            .OrderByDescending(m => m.Rating)
            .Take(10)
            .ToListAsync();
    }

    private static async Task PerformWriteAsync(BenchmarkConfiguration config, int threadId)
    {
        if (config.Architecture == ArchitectureType.SinglePool)
        {
            var factory = config.ServiceProvider.GetRequiredService<IDbContextFactory<SinglePoolDbContext>>();
            await using var context = await factory.CreateDbContextAsync();
            await PerformWriteInternalAsync(context, threadId, config);
        }
        else
        {
            var manager = config.ServiceProvider.GetRequiredService<IDbContextManager>();
            await using var scope = await manager.CreateWriteContextAsync();
            await PerformWriteInternalAsync(scope.Context, threadId, config);
        }
    }

    private static async Task PerformWriteInternalAsync(DbContext context, int threadId, BenchmarkConfiguration config)
    {
        var userId = (threadId % 50) + 1;
        var user = await context.Set<User>().FindAsync(userId);
        if (user != null)
        {
            user.LastLogin = DateTime.UtcNow;
            await SaveChangesAsync(config, context);
        }
    }

    private static async Task PerformContentionWriteAsync(BenchmarkConfiguration config, int threadId, int opIndex)
    {
        if (config.Architecture == ArchitectureType.SinglePool)
        {
            var factory = config.ServiceProvider.GetRequiredService<IDbContextFactory<SinglePoolDbContext>>();
            await using var context = await factory.CreateDbContextAsync();
            await PerformContentionWriteInternalAsync(context, threadId, opIndex, config);
        }
        else
        {
            var manager = config.ServiceProvider.GetRequiredService<IDbContextManager>();
            await using var scope = await manager.CreateWriteContextAsync();
            await PerformContentionWriteInternalAsync(scope.Context, threadId, opIndex, config);
        }
    }

    private static async Task PerformContentionWriteInternalAsync(DbContext context, int threadId, int opIndex, BenchmarkConfiguration config)
    {
        // ALL threads update THE SAME user (id=999)
        var user = await context.Set<User>().FindAsync(999);
        if (user != null)
        {
            user.LastLogin = DateTime.UtcNow;
            user.Email = $"thread{threadId}_op{opIndex}@test.com";

            // Delay to increase lock holding time and force overlap
            await Task.Delay(1);

            await SaveChangesAsync(config, context);
        }
    }

    private static async Task ValidateContentionDataIntegrity(BenchmarkConfiguration config, BenchmarkMetrics metrics)
    {
        if (config.Architecture == ArchitectureType.SinglePool)
        {
            var factory = config.ServiceProvider.GetRequiredService<IDbContextFactory<SinglePoolDbContext>>();
            await using var context = await factory.CreateDbContextAsync();
            await ValidateContentionDataIntegrityInternalAsync(context, config, metrics);
        }
        else
        {
            var manager = config.ServiceProvider.GetRequiredService<IDbContextManager>();
            await using var scope = await manager.CreateWriteContextAsync();
            await ValidateContentionDataIntegrityInternalAsync(scope.Context, config, metrics);
        }
    }

    private static async Task ValidateContentionDataIntegrityInternalAsync(DbContext context, BenchmarkConfiguration config, BenchmarkMetrics metrics)
    {
        var user = await context.Set<User>().FindAsync(999);

        if (user != null)
        {
            // Expected token = number of successful writes (token increments on each save)
            var expectedToken = (uint)metrics.SuccessCount;
            var actualToken = user.ConcurrencyToken;

            metrics.ExpectedConcurrencyToken = expectedToken;
            metrics.ActualConcurrencyToken = actualToken;

            // Track OptimisticLocking retry stats
            if (config.LockingBehavior is OptimisticLockBehavior optimistic)
            {
                metrics.OptimisticRetries = optimistic.TotalRetries;
                metrics.OptimisticConflicts = optimistic.TotalConflicts;
            }
        }
    }

    // Helper methods to abstract architecture differences

    private static async Task<DbContext> CreateReadContextAsync(BenchmarkConfiguration config)
    {
        if (config.Architecture == ArchitectureType.SinglePool)
        {
            var factory = config.ServiceProvider.GetRequiredService<IDbContextFactory<SinglePoolDbContext>>();
            return await factory.CreateDbContextAsync();
        }
        else
        {
            var manager = config.ServiceProvider.GetRequiredService<IDbContextManager>();
            return await manager.CreateReadContextAsync();
        }
    }

    private static async Task SaveChangesAsync(BenchmarkConfiguration config, DbContext context)
    {
        if (config.Architecture == ArchitectureType.SinglePool)
        {
            await ((SinglePoolDbContext)context).SaveChangesAsync();
        }
        else
        {
            await ((WriteDbContext)context).SaveChangesAsync();
        }
    }
}
