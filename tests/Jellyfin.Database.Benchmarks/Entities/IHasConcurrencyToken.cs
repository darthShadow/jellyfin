namespace Jellyfin.Database.Benchmarks.Entities;

/// <summary>
/// Interface for entities that support optimistic concurrency control.
/// </summary>
public interface IHasConcurrencyToken
{
    /// <summary>
    /// Gets the concurrency token for optimistic locking.
    /// </summary>
    uint ConcurrencyToken { get; }

    /// <summary>
    /// Called when SaveChanges is invoked to update the concurrency token.
    /// </summary>
    void OnSavingChanges();
}
