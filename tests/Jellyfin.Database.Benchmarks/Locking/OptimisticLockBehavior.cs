using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Jellyfin.Database.Benchmarks.Locking;

/// <summary>
/// Optimistic locking with retry on concurrency conflicts.
/// Allows concurrent writes but retries on conflicts (SQLITE_BUSY or concurrency token mismatch).
/// </summary>
public class OptimisticLockBehavior : IEntityFrameworkCoreLockingBehavior
{
    private const int MaxRetries = 5;
    private const int BaseDelayMs = 10;

    private int _totalConflicts;
    private int _totalRetries;
    private int _maxRetriesReached;

    /// <inheritdoc />
    public string Name => "Optimistic";

    /// <summary>
    /// Gets the total number of conflicts encountered (DbUpdateConcurrencyException + SQLITE_BUSY).
    /// </summary>
    public int TotalConflicts => _totalConflicts;

    /// <summary>
    /// Gets the total number of retries performed.
    /// </summary>
    public int TotalRetries => _totalRetries;

    /// <summary>
    /// Gets the number of times max retries was reached (operation failed after 5 retries).
    /// </summary>
    public int MaxRetriesReached => _maxRetriesReached;

    /// <summary>
    /// Resets all counters (useful for testing).
    /// </summary>
    public void ResetCounters()
    {
        _totalConflicts = 0;
        _totalRetries = 0;
        _maxRetriesReached = 0;
    }

    /// <inheritdoc />
    public void OnSaveChanges(DbContext context, Action saveChangesAction)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                saveChangesAction();
                return;
            }
            catch (DbUpdateConcurrencyException) when (retryCount < MaxRetries)
            {
                Interlocked.Increment(ref _totalConflicts);
                retryCount++;
                Interlocked.Increment(ref _totalRetries);
                Thread.Sleep(BaseDelayMs * retryCount);

                // Refresh entities to get latest values
                foreach (var entry in context.ChangeTracker.Entries())
                {
                    entry.Reload();
                }
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 5 && retryCount < MaxRetries) // SQLITE_BUSY
            {
                Interlocked.Increment(ref _totalConflicts);
                retryCount++;
                Interlocked.Increment(ref _totalRetries);
                Thread.Sleep(BaseDelayMs * retryCount);
            }
            catch (DbUpdateConcurrencyException) when (retryCount >= MaxRetries)
            {
                Interlocked.Increment(ref _maxRetriesReached);
                throw;
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 5 && retryCount >= MaxRetries)
            {
                Interlocked.Increment(ref _maxRetriesReached);
                throw;
            }
        }
    }

    /// <inheritdoc />
    public async Task OnSaveChangesAsync(DbContext context, Func<Task> saveChangesAction)
    {
        var retryCount = 0;
        while (true)
        {
            try
            {
                await saveChangesAction().ConfigureAwait(false);
                return;
            }
            catch (DbUpdateConcurrencyException) when (retryCount < MaxRetries)
            {
                Interlocked.Increment(ref _totalConflicts);
                retryCount++;
                Interlocked.Increment(ref _totalRetries);
                await Task.Delay(BaseDelayMs * retryCount).ConfigureAwait(false);

                // Refresh entities to get latest values
                foreach (var entry in context.ChangeTracker.Entries())
                {
                    await entry.ReloadAsync().ConfigureAwait(false);
                }
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 5 && retryCount < MaxRetries) // SQLITE_BUSY
            {
                Interlocked.Increment(ref _totalConflicts);
                retryCount++;
                Interlocked.Increment(ref _totalRetries);
                await Task.Delay(BaseDelayMs * retryCount).ConfigureAwait(false);
            }
            catch (DbUpdateConcurrencyException) when (retryCount >= MaxRetries)
            {
                Interlocked.Increment(ref _maxRetriesReached);
                throw;
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 5 && retryCount >= MaxRetries)
            {
                Interlocked.Increment(ref _maxRetriesReached);
                throw;
            }
        }
    }
}
