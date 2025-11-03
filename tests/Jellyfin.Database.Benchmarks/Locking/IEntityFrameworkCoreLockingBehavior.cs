using Microsoft.EntityFrameworkCore;

namespace Jellyfin.Database.Benchmarks.Locking;

/// <summary>
/// Interface for EFCore locking behaviors.
/// </summary>
public interface IEntityFrameworkCoreLockingBehavior
{
    /// <summary>
    /// Gets the name of the locking behavior.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Executes SaveChanges with the appropriate locking strategy.
    /// </summary>
    void OnSaveChanges(DbContext context, Action saveChangesAction);

    /// <summary>
    /// Executes SaveChangesAsync with the appropriate locking strategy.
    /// </summary>
    Task OnSaveChangesAsync(DbContext context, Func<Task> saveChangesAction);
}
