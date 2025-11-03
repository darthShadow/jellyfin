using Jellyfin.Database.Benchmarks.Locking;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Database.Benchmarks.Proposed;

/// <summary>
/// Read-only database context from dedicated reader connection pool.
/// </summary>
public sealed class ReadDbContext : DbContextBase
{
    public ReadDbContext(
        DbContextOptions<ReadDbContext> options,
        ILogger<ReadDbContext> logger,
        IEntityFrameworkCoreLockingBehavior lockingBehavior)
        : base(options, logger, lockingBehavior)
    {
        // Disable change tracking for read-only operations (20-30% performance boost)
        ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
    }

    /// <inheritdoc/>
    public override Task<int> SaveChangesAsync(
        bool acceptAllChangesOnSuccess,
        CancellationToken cancellationToken = default)
    {
        throw new InvalidOperationException(
            "Read-only context cannot save changes. Use WriteDbContext for write operations.");
    }

    /// <inheritdoc/>
    public override int SaveChanges(bool acceptAllChangesOnSuccess)
    {
        throw new InvalidOperationException(
            "Read-only context cannot save changes. Use WriteDbContext for write operations.");
    }
}
