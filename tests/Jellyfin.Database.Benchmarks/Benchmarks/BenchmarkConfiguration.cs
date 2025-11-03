using Jellyfin.Database.Benchmarks.Locking;

namespace Jellyfin.Database.Benchmarks.Benchmarks;

/// <summary>
/// Configuration for a single benchmark run.
/// </summary>
public record BenchmarkConfiguration
{
    public required string Name { get; init; }
    public required ArchitectureType Architecture { get; init; }
    public required IEntityFrameworkCoreLockingBehavior LockingBehavior { get; init; }
    public required IServiceProvider ServiceProvider { get; init; }
}

/// <summary>
/// Type of database architecture.
/// </summary>
public enum ArchitectureType
{
    SinglePool,
    DualPool
}
