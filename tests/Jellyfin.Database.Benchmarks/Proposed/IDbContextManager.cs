namespace Jellyfin.Database.Benchmarks.Proposed;

/// <summary>
/// Manages creation of read and write database contexts from appropriate pools.
/// Write contexts are automatically serialized to prevent concurrent write conflicts.
/// </summary>
public interface IDbContextManager
{
    /// <summary>
    /// Creates a read-only database context from the reader pool.
    /// Multiple reads can execute concurrently.
    /// </summary>
    Task<ReadDbContext> CreateReadContextAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a read-only database context (synchronous).
    /// Multiple reads can execute concurrently.
    /// </summary>
    ReadDbContext CreateReadContext();

    /// <summary>
    /// Creates a serialized write context that enforces single-writer access.
    /// Only one write can execute at a time - others will block until the semaphore is released.
    /// IMPORTANT: Must be used with 'await using' to ensure proper disposal and semaphore release.
    /// </summary>
    /// <example>
    /// await using var scope = await manager.CreateWriteContextAsync();
    /// var user = await scope.Context.Users.FindAsync(id);
    /// user.Email = "new@email.com";
    /// await scope.Context.SaveChangesAsync();
    /// </example>
    Task<SerializedWriteScope> CreateWriteContextAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a serialized write context (synchronous).
    /// Only one write can execute at a time - others will block until the semaphore is released.
    /// </summary>
    SerializedWriteScope CreateWriteContext();
}
