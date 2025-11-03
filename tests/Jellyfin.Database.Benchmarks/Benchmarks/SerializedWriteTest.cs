using Jellyfin.Database.Benchmarks.Entities;
using Jellyfin.Database.Benchmarks.Proposed;
using Microsoft.Extensions.DependencyInjection;

namespace Jellyfin.Database.Benchmarks.Benchmarks;

/// <summary>
/// Validates SerializedWriteScope solution for same-row concurrent updates.
/// Demonstrates 100% data integrity with proper write serialization.
/// </summary>
public static class SerializedWriteTest
{
    public static async Task RunAsync(IServiceProvider serviceProvider)
    {
        Console.WriteLine("\n┌─────────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│  SOLUTION: SerializedWriteScope for Same-Row Updates                       │");
        Console.WriteLine("│  Same scenario as DataIntegrityTest Part 2, but with SerializedWriteScope  │");
        Console.WriteLine("│  Expected: 100% data integrity, both updates succeed, max 1 concurrent ctx │");
        Console.WriteLine("└─────────────────────────────────────────────────────────────────────────────┘\n");

        var manager = serviceProvider.GetRequiredService<IDbContextManager>();

        // Setup: Create test user
        await using (var scope = await manager.CreateWriteContextAsync())
        {
            scope.Context.Users.RemoveRange(scope.Context.Users);
            await scope.Context.SaveChangesAsync();

            scope.Context.Users.Add(new User
            {
                Id = 999,
                Username = "test_user",
                Email = "original@test.com",
                LastLogin = DateTime.UtcNow
            });
            await scope.Context.SaveChangesAsync();
        }

        Console.WriteLine("Initial state: Email='original@test.com', Username='test_user', token=0");
        Console.WriteLine("Running concurrent updates with SerializedWriteScope...");

        // Track concurrency
        int maxConcurrent = 0;
        int currentConcurrent = 0;
        object lockObj = new();

        // Same concurrent API scenario but with SerializedWriteScope
        var thread1Complete = new TaskCompletionSource<bool>();
        var thread2Start = new TaskCompletionSource<bool>();

        var thread1Task = Task.Run(async () =>
        {
            await using var scope = await manager.CreateWriteContextAsync();

            lock (lockObj)
            {
                currentConcurrent++;
                if (currentConcurrent > maxConcurrent)
                    maxConcurrent = currentConcurrent;
            }

            try
            {
                var user = await scope.Context.Users.FindAsync(999);
                user!.Email = "thread1@test.com";

                // Signal thread 2 to start
                thread2Start.SetResult(true);

                // Small delay
                await Task.Delay(10);

                await scope.Context.SaveChangesAsync();
            }
            finally
            {
                lock (lockObj)
                {
                    currentConcurrent--;
                }
            }

            thread1Complete.SetResult(true);
        });

        var thread2Task = Task.Run(async () =>
        {
            // Wait for thread 1 to acquire lock first
            await thread2Start.Task;

            await using var scope = await manager.CreateWriteContextAsync();

            lock (lockObj)
            {
                currentConcurrent++;
                if (currentConcurrent > maxConcurrent)
                    maxConcurrent = currentConcurrent;
            }

            try
            {
                // Wait for thread 1 to complete
                await thread1Complete.Task;

                var user = await scope.Context.Users.FindAsync(999);
                user!.Username = "updated_user";

                await scope.Context.SaveChangesAsync();
            }
            finally
            {
                lock (lockObj)
                {
                    currentConcurrent--;
                }
            }
        });

        await Task.WhenAll(thread1Task, thread2Task);

        // Check final state
        await using (var scope = await manager.CreateWriteContextAsync())
        {
            var user = await scope.Context.Users.FindAsync(999);

            Console.WriteLine($"\n─────────────────────────────────────────────────────────────────────────────");
            Console.WriteLine($"Results:");
            Console.WriteLine($"  Email: {user!.Email}");
            Console.WriteLine($"  Username: {user.Username}");
            Console.WriteLine($"  ConcurrencyToken: {user.ConcurrencyToken}");
            Console.WriteLine($"  Max concurrent contexts: {maxConcurrent}");
            Console.WriteLine($"─────────────────────────────────────────────────────────────────────────────");

            bool emailCorrect = user.Email == "thread1@test.com";
            bool usernameCorrect = user.Username == "updated_user";
            bool tokenCorrect = user.ConcurrencyToken == 2;
            bool serialized = maxConcurrent == 1;

            Console.WriteLine();
            if (emailCorrect && usernameCorrect && tokenCorrect && serialized)
            {
                Console.WriteLine($"✅ SUCCESS: SerializedWriteScope guarantees data integrity");
                Console.WriteLine($"   Both updates applied sequentially with zero data loss");
            }
            else
            {
                Console.WriteLine($"❌ FAILURE: SerializedWriteScope validation failed");
                if (!emailCorrect || !usernameCorrect)
                    Console.WriteLine($"   Data integrity: Email={user.Email}, Username={user.Username}");
                if (!serialized)
                    Console.WriteLine($"   Serialization broken: {maxConcurrent} concurrent contexts");
                if (!tokenCorrect)
                    Console.WriteLine($"   Token mismatch: expected 2, got {user.ConcurrencyToken}");
            }
        }
    }
}
