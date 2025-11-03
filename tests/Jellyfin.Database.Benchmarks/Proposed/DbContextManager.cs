using Microsoft.EntityFrameworkCore;

namespace Jellyfin.Database.Benchmarks.Proposed;

/// <inheritdoc/>
public sealed class DbContextManager : IDbContextManager
{
    private readonly IDbContextFactory<ReadDbContext> _readContextFactory;
    private readonly IDbContextFactory<WriteDbContext> _writeContextFactory;
    private readonly SemaphoreSlim _writeSemaphore = new(1, 1);

    public DbContextManager(
        IDbContextFactory<ReadDbContext> readContextFactory,
        IDbContextFactory<WriteDbContext> writeContextFactory)
    {
        _readContextFactory = readContextFactory;
        _writeContextFactory = writeContextFactory;
    }

    /// <inheritdoc/>
    public Task<ReadDbContext> CreateReadContextAsync(CancellationToken cancellationToken = default)
        => _readContextFactory.CreateDbContextAsync(cancellationToken);

    /// <inheritdoc/>
    public ReadDbContext CreateReadContext()
        => _readContextFactory.CreateDbContext();

    /// <inheritdoc/>
    public Task<SerializedWriteScope> CreateWriteContextAsync(CancellationToken cancellationToken = default)
        => SerializedWriteScope.CreateAsync(_writeContextFactory, _writeSemaphore, cancellationToken);

    /// <inheritdoc/>
    public SerializedWriteScope CreateWriteContext()
        => SerializedWriteScope.Create(_writeContextFactory, _writeSemaphore);
}
