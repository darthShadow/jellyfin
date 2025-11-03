using Jellyfin.Database.Benchmarks.Locking;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Database.Benchmarks.Proposed;

/// <summary>
/// Service setup for dual-pool architecture.
/// </summary>
public static class DualPoolSetup
{
    public static IServiceProvider CreateServiceProvider(
        string dbPath,
        IEntityFrameworkCoreLockingBehavior lockingBehavior,
        int readerPoolSize = 8,
        int writerPoolSize = 1)
    {
        var services = new ServiceCollection();

        // Logging
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));

        // Locking behavior
        services.AddSingleton(lockingBehavior);

        // Reader pool (optimized for concurrent reads)
        services.AddPooledDbContextFactory<ReadDbContext>(
            (serviceProvider, options) =>
            {
                var connectionString = new SqliteConnectionStringBuilder
                {
                    DataSource = dbPath,
                    Mode = SqliteOpenMode.ReadWrite, // Must be ReadWrite for WAL mode
                    Cache = SqliteCacheMode.Shared,
                    Pooling = true
                }.ToString();

                options
                    .UseSqlite(connectionString)
                    .AddInterceptors(new SqlitePragmaInterceptor(busyTimeout: 5000))
                    .LogTo(Console.WriteLine, LogLevel.Warning);

                // Note: In production, reader pragmas would be set via interceptor
                // query_only=1, cache_size=-131072 (128MB), threads=4, etc.
            },
            poolSize: readerPoolSize);

        // Writer pool (single connection for writes)
        services.AddPooledDbContextFactory<WriteDbContext>(
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

                // Note: In production, writer pragmas would be set via interceptor
                // auto_vacuum=INCREMENTAL, foreign_keys=ON, cache_size=-65536 (64MB), etc.
            },
            poolSize: writerPoolSize);

        // Context manager
        services.AddSingleton<IDbContextManager, DbContextManager>();

        return services.BuildServiceProvider();
    }

    public static async Task InitializeDatabaseAsync(IServiceProvider serviceProvider)
    {
        var manager = serviceProvider.GetRequiredService<IDbContextManager>();
        await using var scope = await manager.CreateWriteContextAsync();

        await scope.Context.Database.EnsureDeletedAsync();
        await scope.Context.Database.EnsureCreatedAsync();

        // Set SQLite pragmas
        await scope.Context.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
        await scope.Context.Database.ExecuteSqlRawAsync("PRAGMA busy_timeout=5000;");
        await scope.Context.Database.ExecuteSqlRawAsync("PRAGMA synchronous=NORMAL;");
        await scope.Context.Database.ExecuteSqlRawAsync("PRAGMA temp_store=MEMORY;");

        // Reader-specific pragmas (would be set per-connection in production)
        await scope.Context.Database.ExecuteSqlRawAsync("PRAGMA cache_size=-131072;"); // 128MB for readers

        // Writer-specific pragmas
        await scope.Context.Database.ExecuteSqlRawAsync("PRAGMA auto_vacuum=INCREMENTAL;");
        await scope.Context.Database.ExecuteSqlRawAsync("PRAGMA foreign_keys=ON;");
    }
}
