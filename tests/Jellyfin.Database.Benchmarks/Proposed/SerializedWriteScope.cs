namespace Jellyfin.Database.Benchmarks.Proposed;

/// <summary>
/// Scope that enforces serialized write access by holding a semaphore.
/// Disposed when the context is disposed, releasing the semaphore.
/// </summary>
public sealed class SerializedWriteScope : IAsyncDisposable, IDisposable
{
    private readonly WriteDbContext _context;
    private readonly SemaphoreSlim _semaphore;
    private bool _disposed;

    private SerializedWriteScope(WriteDbContext context, SemaphoreSlim semaphore)
    {
        _context = context;
        _semaphore = semaphore;
    }

    /// <summary>
    /// Acquires the write semaphore and creates a context.
    /// </summary>
    public static async Task<SerializedWriteScope> CreateAsync(
        Microsoft.EntityFrameworkCore.IDbContextFactory<WriteDbContext> factory,
        SemaphoreSlim semaphore,
        CancellationToken cancellationToken = default)
    {
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var context = await factory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
            return new SerializedWriteScope(context, semaphore);
        }
        catch
        {
            semaphore.Release();
            throw;
        }
    }

    /// <summary>
    /// Acquires the write semaphore and creates a context (synchronous).
    /// </summary>
    public static SerializedWriteScope Create(
        Microsoft.EntityFrameworkCore.IDbContextFactory<WriteDbContext> factory,
        SemaphoreSlim semaphore)
    {
        semaphore.Wait();
        try
        {
            var context = factory.CreateDbContext();
            return new SerializedWriteScope(context, semaphore);
        }
        catch
        {
            semaphore.Release();
            throw;
        }
    }

    /// <summary>
    /// Gets the write context. Use with 'await using' to ensure proper disposal.
    /// </summary>
    public WriteDbContext Context => _context;

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                _context.Dispose();
            }
            finally
            {
                _semaphore.Release();
                _disposed = true;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            try
            {
                await _context.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _semaphore.Release();
                _disposed = true;
            }
        }
    }
}
