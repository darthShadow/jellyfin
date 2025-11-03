using Jellyfin.Database.Benchmarks.Locking;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Database.Benchmarks.Current;

/// <summary>
/// Service setup for single-pool architecture.
/// </summary>
public static class SinglePoolSetup
{
    public static IServiceProvider CreateServiceProvider(
        string dbPath,
        IEntityFrameworkCoreLockingBehavior lockingBehavior,
        int poolSize = 4)
    {
        var services = new ServiceCollection();

        // Logging
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Locking behavior
        services.AddSingleton(lockingBehavior);

        // Single DbContext pool (used for both reads and writes)
        services.AddPooledDbContextFactory<SinglePoolDbContext>(
            (serviceProvider, options) =>
            {
                var connectionString = new SqliteConnectionStringBuilder
                {
                    DataSource = dbPath,
                    Mode = SqliteOpenMode.ReadWriteCreate,
                    Cache = SqliteCacheMode.Shared,
                    Pooling = true
                }.ToString();

                options
                    .UseSqlite(connectionString)
                    .AddInterceptors(new SqlitePragmaInterceptor(busyTimeout: 5000))
                    .LogTo(Console.WriteLine, LogLevel.Warning);
            },
            poolSize: poolSize);

        return services.BuildServiceProvider();
    }

    public static async Task InitializeDatabaseAsync(IServiceProvider serviceProvider)
    {
        var factory = serviceProvider.GetRequiredService<IDbContextFactory<SinglePoolDbContext>>();
        await using var context = await factory.CreateDbContextAsync();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Set SQLite pragmas
        await context.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
        await context.Database.ExecuteSqlRawAsync("PRAGMA busy_timeout=5000;");
        await context.Database.ExecuteSqlRawAsync("PRAGMA synchronous=NORMAL;");
        await context.Database.ExecuteSqlRawAsync("PRAGMA cache_size=-65536;"); // 64MB
        await context.Database.ExecuteSqlRawAsync("PRAGMA temp_store=MEMORY;");
    }
}
