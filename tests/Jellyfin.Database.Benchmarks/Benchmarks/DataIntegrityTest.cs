using Jellyfin.Database.Benchmarks.Current;
using Jellyfin.Database.Benchmarks.Entities;
using Jellyfin.Database.Benchmarks.Proposed;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Jellyfin.Database.Benchmarks.Benchmarks;

/// <summary>
/// Data integrity problems with current Jellyfin architecture:
/// 1. Pool concurrency test (proves poolSize doesn't limit concurrency)
/// 2. Current Jellyfin behavior (proves DbUpdateConcurrencyException causes data loss)
/// </summary>
public static class DataIntegrityTest
{
    public static async Task RunAsync(BenchmarkConfiguration config)
    {
        await TestPoolConcurrency(config);
        await TestOptimisticLockingFailure(config);
    }

    private static async Task TestPoolConcurrency(BenchmarkConfiguration config)
    {
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  PART 1: Pool Concurrency Test                                             │");
        Console.WriteLine("│  Tests if poolSize=1 actually serializes write operations                  │");
        Console.WriteLine("│  Expected: poolSize is for reuse, NOT concurrency control                  │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        // Setup: Create hotspot user
        await ExecuteWithWriteContextAsync(config, async ctx =>
        {
            ctx.Set<User>().RemoveRange(ctx.Set<User>());
            await ctx.SaveChangesAsync();

            ctx.Set<User>().Add(new User
            {
                Id = 999,
                Username = "hotspot_user",
                Email = "hotspot@test.com",
                LastLogin = DateTime.UtcNow
            });
            await ctx.SaveChangesAsync();
        });

        // Track concurrency
        int maxConcurrent = 0;
        int currentConcurrent = 0;
        object lockObj = new();

        Console.WriteLine("Running 10 concurrent write operations...");

        var tasks = Enumerable.Range(0, 10)
            .Select(async threadId =>
            {
                await ExecuteWithWriteContextAsync(config, async context =>
                {
                    lock (lockObj)
                    {
                        currentConcurrent++;
                        if (currentConcurrent > maxConcurrent)
                            maxConcurrent = currentConcurrent;
                    }

                    try
                    {
                        var user = await context.Set<User>().FindAsync(999);
                        if (user != null)
                        {
                            await Task.Delay(10); // Force overlap
                            user.LastLogin = DateTime.UtcNow;
                            await context.SaveChangesAsync();
                        }
                    }
                    catch (Exception)
                    {
                        // Expected: DbUpdateConcurrencyException or SQLITE_BUSY
                        // This test demonstrates concurrent writes can fail
                    }
                    finally
                    {
                        lock (lockObj)
                        {
                            currentConcurrent--;
                        }
                    }
                });
            })
            .ToArray();

        await Task.WhenAll(tasks);

        Console.WriteLine($"\n─────────────────────────────────────────────────────────────────────────────");
        Console.WriteLine($"Results:");
        Console.WriteLine($"  Max concurrent contexts: {maxConcurrent}");
        Console.WriteLine($"  Expected with poolSize=1: 1");
        Console.WriteLine($"  Writer pool size configured: 1");
        Console.WriteLine($"─────────────────────────────────────────────────────────────────────────────");

        Console.WriteLine();
        if (maxConcurrent > 1)
        {
            Console.WriteLine($"❌ FAIL: poolSize=1 created {maxConcurrent} concurrent contexts");
            Console.WriteLine($"   This proves poolSize is for instance reuse, NOT concurrency control.");
        }
        else
        {
            Console.WriteLine($"✅ PASS: Only 1 concurrent context (unexpected!)");
        }
    }

    private static async Task TestOptimisticLockingFailure(BenchmarkConfiguration config)
    {
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  PART 2: Current Jellyfin Behavior - DbUpdateConcurrencyException          │");
        Console.WriteLine("│  Two threads update the same user simultaneously                            │");
        Console.WriteLine("│  Expected: Second thread gets DbUpdateConcurrencyException, update is lost │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        // Setup: Create test user
        await ExecuteWithWriteContextAsync(config, async ctx =>
        {
            ctx.Set<User>().RemoveRange(ctx.Set<User>());
            await ctx.SaveChangesAsync();

            ctx.Set<User>().Add(new User
            {
                Id = 999,
                Username = "test_user",
                Email = "original@test.com",
                LastLogin = DateTime.UtcNow
            });
            await ctx.SaveChangesAsync();
        });

        Console.WriteLine("Initial state: Email='original@test.com', Username='test_user', token=0");
        Console.WriteLine("Running concurrent updates: Thread 1 (Email), Thread 2 (Username)...");

        // Simulate two concurrent API requests (e.g., updating user configuration)
        var thread1Complete = new TaskCompletionSource<bool>();
        var thread2Start = new TaskCompletionSource<bool>();
        bool thread2Failed = false;
        string? thread2Error = null;

        var thread1Task = Task.Run(async () =>
        {
            await ExecuteWithWriteContextAsync(config, async ctx =>
            {
                var user = await ctx.Set<User>().FindAsync(999);
                user!.Email = "thread1@test.com";

                // Signal thread 2 to start (both have read the same initial state)
                thread2Start.SetResult(true);

                // Small delay to let thread 2 also modify its property
                await Task.Delay(10);

                await ctx.SaveChangesAsync();
                thread1Complete.SetResult(true);
            });
        });

        var thread2Task = Task.Run(async () =>
        {
            // Wait for thread 1 to read first
            await thread2Start.Task;

            await ExecuteWithWriteContextAsync(config, async ctx =>
            {
                var user = await ctx.Set<User>().FindAsync(999);
                user!.Username = "updated_user";

                // Wait for thread 1 to save first
                await thread1Complete.Task;

                try
                {
                    await ctx.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException ex)
                {
                    thread2Failed = true;
                    thread2Error = ex.GetType().Name;
                }
            });
        });

        await Task.WhenAll(thread1Task, thread2Task);

        // Check final state
        await ExecuteWithWriteContextAsync(config, async ctx =>
        {
            var user = await ctx.Set<User>().FindAsync(999);

            Console.WriteLine($"\n─────────────────────────────────────────────────────────────────────────────");
            Console.WriteLine($"Results:");
            Console.WriteLine($"  Email: {user!.Email}");
            Console.WriteLine($"  Username: {user.Username}");
            Console.WriteLine($"  ConcurrencyToken: {user.ConcurrencyToken}");
            Console.WriteLine($"  Thread 2 exception: {(thread2Failed ? thread2Error : "None")}");
            Console.WriteLine($"─────────────────────────────────────────────────────────────────────────────");

            bool emailCorrect = user.Email == "thread1@test.com";
            bool usernameCorrect = user.Username == "updated_user";

            Console.WriteLine();
            if (thread2Failed && !usernameCorrect)
            {
                Console.WriteLine($"❌ EXPECTED BEHAVIOR: Data loss due to unhandled DbUpdateConcurrencyException");
                Console.WriteLine($"   Thread 1: Email updated successfully");
                Console.WriteLine($"   Thread 2: Username update lost (exception not retried)");
            }
            else if (!thread2Failed && usernameCorrect)
            {
                Console.WriteLine($"✅ Both changes applied (locking behavior prevented the conflict)");
            }
            else
            {
                Console.WriteLine($"⚠️  Unexpected result - check locking configuration");
            }
        });
    }

    // Helper methods to abstract architecture differences
    private static async Task ExecuteWithWriteContextAsync(
        BenchmarkConfiguration config,
        Func<DbContext, Task> action)
    {
        if (config.Architecture == ArchitectureType.SinglePool)
        {
            var factory = config.ServiceProvider.GetRequiredService<IDbContextFactory<SinglePoolDbContext>>();
            await using var context = await factory.CreateDbContextAsync();
            await action(context);
        }
        else
        {
            var manager = config.ServiceProvider.GetRequiredService<IDbContextManager>();
            await using var scope = await manager.CreateWriteContextAsync();
            await action(scope.Context);
        }
    }
}
