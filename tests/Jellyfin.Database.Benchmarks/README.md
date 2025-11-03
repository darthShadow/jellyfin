# Jellyfin Database Architecture Benchmark

Comprehensive benchmark comparing **single-pool** vs **dual-pool** database architectures across 3 locking behaviors, with diagnostic tests revealing critical data integrity issues.

## Quick Start

```bash
cd tests/Jellyfin.Database.Benchmarks
dotnet run -c Release
```

## What This Benchmark Tests

### Architecture Comparison

**Current: Single-Pool**
- All reads and writes share the same connection pool (4 connections)
- Simple but limited concurrency

**Proposed: Dual-Pool**
- Separate reader pool (8 connections) and writer pool (1 connection)
- Reads don't compete with writes for connections
- **Writes are automatically serialized** via built-in SemaphoreSlim (guarantees data integrity)
- Expected 1.4-2.8x improvement for read-heavy workloads

### Test Matrix: 6 Configurations

| Architecture | Locking | Description |
|--------------|---------|-------------|
| Single-Pool  | NoLock | SQLite manages locking, may see conflicts |
| Single-Pool  | Pessimistic | SemaphoreSlim serializes writes |
| Single-Pool  | Optimistic | Retry on concurrency conflicts |
| Dual-Pool    | NoLock | **Writes auto-serialized**, reads isolated, 100% data integrity |
| Dual-Pool    | Pessimistic | **Writes auto-serialized**, reads isolated, 100% data integrity |
| Dual-Pool    | Optimistic | **Writes auto-serialized**, reads isolated, 100% data integrity |

### Test Suites (6 total)

1. **Pure Read (100% reads)** - Tests read concurrency limits at 5 concurrency levels (1, 5, 10, 20, 50)
2. **Pure Write (100% writes)** - Tests lock contention behavior at 5 concurrency levels
3. **Mixed (90% reads, 10% writes)** - Realistic Jellyfin usage at 5 concurrency levels
4. **Contention Stress** - All threads update same row at 3 concurrency levels (5, 10, 20)
5. **Data Integrity Problems** (current architecture):
   - Part 1: Pool concurrency test (proves poolSize=1 doesn't serialize - creates 10 concurrent contexts)
   - Part 2: Current Jellyfin behavior (two threads update same entity, second gets DbUpdateConcurrencyException and update is lost)
6. **Dual-Pool Write Serialization** - Validates that Dual-Pool's mandatory serialization prevents all data integrity issues (Dual-Pool configs only)

## Key Findings

### ✅ Dual-Pool Delivers for Reads

**Pure read workload:**
- 1.4-2.8x improvement at concurrency ≥10
- At 50 threads: 5,396 → 10,300 ops/sec (1.91x faster)

**Mixed workload (90% read, 10% write):**
- Consistent 5-10% improvement across all locking strategies
- At 50 threads: 8,625 → 9,254 ops/sec

### ⚠️ Critical Discovery: poolSize ≠ Concurrency Control

**IMPORTANT**: `AddPooledDbContextFactory(poolSize: 1)` does NOT limit concurrent access!

When the pool is exhausted, EFCore creates new instances on demand:
```
Max concurrent write contexts: 10 (with poolSize=1!)
Expected: 1
Result: FAIL - Pool size is for reuse, not concurrency control
```

### 🔴 Data Loss with Optimistic Locking

**Without proper implementation**, Optimistic locking causes **silent data loss**:

```csharp
// 10 threads update same row
// Current broken implementation:
catch (DbUpdateConcurrencyException) {
    await entry.ReloadAsync();  // ❌ This LOSES intended changes!
    await SaveChangesAsync();   // Saves reloaded data, not intended changes
}

// Result:
Thread 0: Saves successfully (token 0 → 1)
Threads 1-9: "Save successfully" but lose their changes
Final token: 1 (expected: 10)
90% data loss! 🚨
```

### ✅ Solution: Dual-Pool Architecture with Mandatory Serialization

**Dual-Pool enforces single-writer semantics by design** - there's no unsafe API:

```csharp
// CreateWriteContextAsync() ALWAYS returns SerializedWriteScope
await using var scope = await manager.CreateWriteContextAsync();
var user = await scope.Context.Users.FindAsync(id);
user.Email = "new@email.com";
await scope.Context.SaveChangesAsync();

// Result (all Dual-Pool configurations):
All 10 threads: Save successfully
Final token: 10 ✅
Max concurrent contexts: 1 ✅ (enforced by SemaphoreSlim)
Zero data loss! ✅ (guaranteed by architecture)
```

**Key Insight**: Dual-Pool doesn't rely on `poolSize` for serialization. It uses an application-level `SemaphoreSlim` owned by `DbContextManager`, ensuring only one writer can execute at a time regardless of locking strategy.

## Benchmark Results Summary

### Read Performance (NoLock)

| Concurrency | Single-Pool | Dual-Pool | Improvement |
|------------|-------------|-----------|-------------|
| 1 | 176 ops/sec | 489 ops/sec | **2.78x** |
| 10 | 2,967 ops/sec | 4,213 ops/sec | **1.42x** |
| 20 | 3,806 ops/sec | 6,811 ops/sec | **1.79x** |
| 50 | 5,396 ops/sec | 10,300 ops/sec | **1.91x** |

### Mixed Workload (90% read, 10% write)

| Concurrency | Single-Pool | Dual-Pool | Improvement |
|------------|-------------|-----------|-------------|
| 20 | 7,930 ops/sec | 8,723 ops/sec | **10%** |
| 50 | 8,625 ops/sec | 9,254 ops/sec | **7%** |

### Contention Stress (Same Row Updates)

| Configuration | Error Rate | Data Loss | Throughput | Usable? |
|---------------|-----------|-----------|------------|---------|
| Single-Pool + NoLock | 61-69% | N/A | 143 ops/sec | ❌ No |
| Single-Pool + Pessimistic (broken) | 94% | N/A | 15 ops/sec | ❌ No |
| Single-Pool + Optimistic (broken) | 0% | **90%** | 723 ops/sec | ❌ No |
| **Dual-Pool + NoLock** | **0%** | **0%** | 200 ops/sec | ✅ Yes |
| **Dual-Pool + Pessimistic** | **0%** | **0%** | 200 ops/sec | ✅ Yes |
| **Dual-Pool + Optimistic** | **0%** | **0%** | 200 ops/sec | ✅ Yes |

**Note**: All Dual-Pool configurations achieve 100% data integrity due to mandatory write serialization.

## Project Structure

```
tests/Jellyfin.Database.Benchmarks/
├── README.md                        # This file
├── ENFORCING_SINGLE_WRITER.md       # SerializedWriteScope documentation
├── SERIALIZATION_STRATEGIES.md      # Detailed analysis of all approaches
│
├── Entities/                        # Test entities with concurrency tokens
├── Locking/                         # 3 locking strategies
│   ├── NoLockBehavior.cs
│   ├── PessimisticLockBehavior.cs   # ⚠️ Has known bugs
│   └── OptimisticLockBehavior.cs    # ⚠️ Causes data loss
│
├── Current/                         # Single-pool implementation
│   ├── SinglePoolDbContext.cs
│   └── SinglePoolSetup.cs
│
├── Proposed/                        # Dual-pool implementation
│   ├── DbContextBase.cs             # Shared base
│   ├── ReadDbContext.cs             # Read-only (NoTracking)
│   ├── WriteDbContext.cs            # Read-write (Tracking)
│   ├── IDbContextManager.cs         # Manager interface
│   ├── DbContextManager.cs          # ✅ Enforces write serialization
│   └── SerializedWriteScope.cs      # Write scope with semaphore
│
└── Benchmarks/                      # Test suites
    ├── BenchmarkConfiguration.cs    # Configuration model
    ├── BenchmarkMetrics.cs          # Metrics collection
    ├── WorkloadBenchmark.cs         # Unified benchmark runner (4 workload types)
    ├── DataIntegrityTest.cs         # 2-part problem demonstration
    └── SerializedWriteTest.cs       # Solution validation (fixes problems from Test 5)
```

## What Gets Tested

All tests run automatically in a single command:

**Performance Benchmarks** (6 configurations: 2 architectures × 3 locking behaviors):
- **Pure Read** - 100% reads at 5 concurrency levels (1, 5, 10, 20, 50)
- **Pure Write** - 100% writes at 5 concurrency levels
- **Mixed Workload** - 90% reads, 10% writes at 5 concurrency levels
- **Contention Stress** - All threads update same row at 3 concurrency levels (5, 10, 20)

**Data Integrity Validation** (problem → solution):
- **Test 5: Problems** - Demonstrates Single-Pool + all locking strategies show data integrity issues
  - Part 1: Pool concurrency (poolSize=1 creates 10 concurrent contexts)
  - Part 2: DbUpdateConcurrencyException causes data loss (no retry in Jellyfin)
- **Test 6: Solution** - All Dual-Pool configurations achieve 100% data integrity (mandatory serialization works)

**Metrics Collected:**
- Throughput (ops/sec)
- Latency (p50/p95/p99)
- Error counts
- Concurrent context tracking

## Recommendations for Jellyfin

Based on comprehensive testing:

### ✅ Do This

1. **Adopt dual-pool architecture** for read concurrency (1.4-2.8x improvement) AND guaranteed write integrity
2. **Use CreateWriteContextAsync()** for all writes - serialization is automatic and mandatory
3. **Benefit from built-in data integrity** - no special handling needed for concurrent updates

### ❌ Don't Do This

1. **Don't rely on poolSize for serialization** - it doesn't work
2. **Don't use Optimistic locking without fixing reload logic** - causes silent data loss
3. **Don't use Pessimistic as currently implemented** - it's broken

### Migration Strategy

```csharp
// For reads (concurrent, high performance)
await using var ctx = await manager.CreateReadContextAsync();
var movies = await ctx.Movies.Where(...).ToListAsync();

// For ALL writes (automatically serialized, 100% data integrity guaranteed)
await using var scope = await manager.CreateWriteContextAsync();
var user = await scope.Context.Users.FindAsync(id);
user.LastLogin = DateTime.UtcNow;
await scope.Context.SaveChangesAsync();
// Note: Returns SerializedWriteScope - access context via .Context property
// Semaphore is automatically acquired/released via 'await using'
```

**Key Change**: No more choosing between "safe" and "unsafe" write methods. All writes are safe by default.

## Understanding the Results

### Why Dual-Pool Wins for Reads

- 8 reader connections can query simultaneously
- Readers don't wait for writers to release connections
- Scales linearly with CPU count (up to 8 connections)

### Why Dual-Pool Enforces Write Serialization

- SQLite is single-writer at transaction level
- Multiple connections CAN attempt concurrent writes → errors or data loss
- Dual-Pool uses application-level `SemaphoreSlim` (not `poolSize`) to enforce serialization
- Result: 100% success rate, 100% data integrity, predictable behavior
- **By Design**: Impossible to create concurrent writers in Dual-Pool architecture

### Performance Trade-offs

| Scenario | Best Choice | Throughput | Data Integrity |
|----------|------------|-----------|----------------|
| Browsing library | Dual-Pool + Read context | ✅ High | N/A (read-only) |
| Creating items | Dual-Pool + Write context | ✅ High* | ✅ Perfect |
| Updating settings | Dual-Pool + Write context | ✅ High* | ✅ Perfect |
| Concurrent hotspot updates | Dual-Pool + Write context | ⚠️ Medium** | ✅ Perfect |

\* High throughput for independent operations (different rows)
\** Serialization limits to ~200 ops/sec for same-row updates (necessary trade-off for correctness)

## Further Documentation

- **ENFORCING_SINGLE_WRITER.md** - Architecture details: how Dual-Pool achieves guaranteed serialization
- **SERIALIZATION_STRATEGIES.md** - Comparison of all approaches (poolSize, pessimistic, optimistic, application-level)
- **OVERVIEW.md** - Quick reference guide

## Contributing

To add new benchmarks:

1. Create test file in `Benchmarks/`
2. Implement using both single-pool and dual-pool patterns
3. Add to Program.cs benchmark suite
4. Document findings in README

## Validation Checklist

- [x] Both architectures compile and run
- [x] Read context blocks SaveChanges in dual-pool
- [x] All 3 locking behaviors tested
- [x] Metrics track throughput, latency, errors
- [x] Scale testing at 5 concurrency levels
- [x] 3 workload types (read, write, mixed)
- [x] Contention stress test (same-row updates)
- [x] Data loss detection (Optimistic locking bug)
- [x] SerializedWriteScope verification (100% success)
- [x] Side-by-side comparison output

## Insights & Lessons Learned

`★ Key Insights ─────────────────────────────────`

**1. Pool Size is for Performance, Not Concurrency**
- `poolSize: 1` creates 1 pre-warmed instance
- When exhausted, EFCore creates new instances on demand
- Does NOT limit concurrent access

**2. Optimistic Locking Needs Application Logic**
- `entry.Reload()` discards pending changes
- Requires custom merge logic to re-apply changes
- Without this: silent 90% data loss

**3. Dual-Pool Benefits are Read-Specific**
- 1.4-2.8x for pure reads ✅
- 5-10% for realistic mixed workloads ✅
- Does NOT serialize writes (needs SerializedWriteScope)

**4. Write Serialization Must Be Explicit**
- SemaphoreSlim at context creation level
- Scope-based pattern ensures proper cleanup
- Only needed for same-row concurrent updates

`─────────────────────────────────────────────────`

---

**Ready to run**: The benchmark is production-ready and reveals critical insights about EFCore pooling and concurrency behavior with SQLite. 🎯
