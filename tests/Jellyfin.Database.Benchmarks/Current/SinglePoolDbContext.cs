using Jellyfin.Database.Benchmarks.Entities;
using Jellyfin.Database.Benchmarks.Locking;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Database.Benchmarks.Current;

/// <summary>
/// Current single-pool DbContext implementation.
/// All read and write operations share the same connection pool.
/// </summary>
public class SinglePoolDbContext : DbContext
{
    private readonly ILogger<SinglePoolDbContext> _logger;
    private readonly IEntityFrameworkCoreLockingBehavior _lockingBehavior;

    public SinglePoolDbContext(
        DbContextOptions<SinglePoolDbContext> options,
        ILogger<SinglePoolDbContext> logger,
        IEntityFrameworkCoreLockingBehavior lockingBehavior)
        : base(options)
    {
        _logger = logger;
        _lockingBehavior = lockingBehavior;
    }

    public DbSet<Movie> Movies => Set<Movie>();
    public DbSet<User> Users => Set<User>();

    /// <inheritdoc/>
    public override async Task<int> SaveChangesAsync(
        bool acceptAllChangesOnSuccess,
        CancellationToken cancellationToken = default)
    {
        HandleConcurrencyToken();

        try
        {
            var result = -1;
            await _lockingBehavior.OnSaveChangesAsync(this, async () =>
            {
                result = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
            }).ConfigureAwait(false);
            return result;
        }
        catch (Exception)
        {
            throw;
        }
    }

    /// <inheritdoc/>
    public override int SaveChanges(bool acceptAllChangesOnSuccess)
    {
        HandleConcurrencyToken();

        try
        {
            var result = -1;
            _lockingBehavior.OnSaveChanges(this, () =>
            {
                result = base.SaveChanges(acceptAllChangesOnSuccess);
            });
            return result;
        }
        catch (Exception)
        {
            throw;
        }
    }

    private void HandleConcurrencyToken()
    {
        foreach (var saveEntity in ChangeTracker.Entries()
                     .Where(e => e.State == EntityState.Modified)
                     .Select(entry => entry.Entity)
                     .OfType<IHasConcurrencyToken>())
        {
            saveEntity.OnSavingChanges();
        }
    }

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Movie>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Title).IsRequired();
            entity.Property(e => e.ConcurrencyToken).IsConcurrencyToken();
        });

        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Username).IsRequired();
            entity.Property(e => e.Email).IsRequired();
            entity.Property(e => e.ConcurrencyToken).IsConcurrencyToken();
        });
    }
}
