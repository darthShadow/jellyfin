using Microsoft.EntityFrameworkCore;

namespace Jellyfin.Database.Benchmarks.Locking;

/// <summary>
/// No application-level locking - relies on SQLite's internal locking.
/// Best performance but may encounter SQLITE_BUSY under high contention.
/// </summary>
public class NoLockBehavior : IEntityFrameworkCoreLockingBehavior
{
    /// <inheritdoc />
    public string Name => "NoLock";

    /// <inheritdoc />
    public void OnSaveChanges(DbContext context, Action saveChangesAction)
    {
        // No locking - just execute the save
        saveChangesAction();
    }

    /// <inheritdoc />
    public async Task OnSaveChangesAsync(DbContext context, Func<Task> saveChangesAction)
    {
        // No locking - just execute the save
        await saveChangesAction().ConfigureAwait(false);
    }
}
