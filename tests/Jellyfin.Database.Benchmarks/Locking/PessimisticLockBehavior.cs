using Microsoft.EntityFrameworkCore;

namespace Jellyfin.Database.Benchmarks.Locking;

/// <summary>
/// Pessimistic locking using SemaphoreSlim to serialize all write operations.
/// Guarantees no write conflicts but reduces concurrency.
/// </summary>
public class PessimisticLockBehavior : IEntityFrameworkCoreLockingBehavior
{
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    /// <inheritdoc />
    public string Name => "Pessimistic";

    /// <inheritdoc />
    public void OnSaveChanges(DbContext context, Action saveChangesAction)
    {
        _writeLock.Wait();
        try
        {
            saveChangesAction();
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <inheritdoc />
    public async Task OnSaveChangesAsync(DbContext context, Func<Task> saveChangesAction)
    {
        await _writeLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await saveChangesAction().ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }
}
