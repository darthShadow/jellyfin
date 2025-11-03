using Jellyfin.Database.Benchmarks.Locking;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Database.Benchmarks.Proposed;

/// <summary>
/// Read-write database context from dedicated writer connection pool.
/// </summary>
public sealed class WriteDbContext : DbContextBase
{
    public WriteDbContext(
        DbContextOptions<WriteDbContext> options,
        ILogger<WriteDbContext> logger,
        IEntityFrameworkCoreLockingBehavior lockingBehavior)
        : base(options, logger, lockingBehavior)
    {
        // Keep default tracking behavior for write operations
    }

    // Inherits SaveChanges methods from base - allows writes
}
