# Jellyfin Database Operational Guide

Comprehensive guide to database usage, SQLite configuration, connection management, and concurrency control in Jellyfin.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Connection Pooling & Lifecycle](#connection-pooling--lifecycle)
3. [SQLite Configuration & Pragmas](#sqlite-configuration--pragmas)
4. [Concurrency Strategies](#concurrency-strategies)
5. [Transaction Management](#transaction-management)
6. [Performance & Optimization](#performance--optimization)
7. [Backup & Restore](#backup--restore)
8. [Configuration Reference](#configuration-reference)

---

## Architecture Overview

### Technology Stack
- **Framework**: .NET 9.0 with ASP.NET Core
- **ORM**: Entity Framework Core (EFCore)
- **Database**: SQLite (default), with pluggable provider architecture
- **Connection Management**: EFCore Pooled DbContext Factory

### Provider Architecture

Jellyfin uses a pluggable database provider system that allows switching between different database backends:

**Built-in Provider:**
- `Jellyfin-SQLite` (default) - SQLite implementation

**Custom Providers:**
- Plugin-based system for third-party database backends
- Loaded dynamically from plugins directory

**Key Files:**
- `SqliteDatabaseProvider.cs` - SQLite provider implementation
- `IJellyfinDatabaseProvider.cs` - Provider interface
- `ServiceCollectionExtensions.cs` - Provider registration and DI setup

### Provider Selection

```csharp
// Priority order:
1. Configuration file: configurationManager.GetConfiguration<DatabaseConfigurationOptions>("database")
2. Command-line argument: --migration-provider
3. Default: SQLite with NoLock behavior
```

**Configuration Structure:**
```csharp
public class DatabaseConfigurationOptions
{
    public required string DatabaseType { get; set; }  // "Jellyfin-SQLite"
    public CustomDatabaseOptions? CustomProviderOptions { get; set; }
    public DatabaseLockingBehaviorTypes LockingBehavior { get; set; }  // NoLock, Pessimistic, Optimistic
}
```

---

## Connection Pooling & Lifecycle

### Connection Pool Configuration

Jellyfin uses **EFCore Pooled DbContext Factory** for efficient connection management.

**Registration (ServiceCollectionExtensions.cs:139):**
```csharp
serviceCollection.AddPooledDbContextFactory<JellyfinDbContext>((serviceProvider, opt) =>
{
    var provider = serviceProvider.GetRequiredService<IJellyfinDatabaseProvider>();
    provider.Initialise(opt, efCoreConfiguration);
    var lockingBehavior = serviceProvider.GetRequiredService<IEntityFrameworkCoreLockingBehavior>();
    lockingBehavior.Initialise(opt);
});
```

### SQLite Connection String

**Default Configuration (SqliteDatabaseProvider.cs:63-66):**
```csharp
var sqliteConnectionBuilder = new SqliteConnectionStringBuilder();
sqliteConnectionBuilder.DataSource = Path.Combine(_applicationPaths.DataPath, "jellyfin.db");
sqliteConnectionBuilder.Cache = SqliteCacheMode.Default;  // Configurable
sqliteConnectionBuilder.Pooling = true;                   // Enabled by default
```

**Configurable Options:**
- `cache` - SQLite cache mode (Default, Private, Shared)
- `pooling` - Enable/disable connection pooling (default: `true`)

### Connection Lifecycle

**Connection Creation:**
- Contexts are created on-demand from the factory
- Each context gets a connection from the pool

**Connection Open Event:**
- `PragmaConnectionInterceptor` executes on every connection open
- Applies configured SQLite pragmas automatically
- Ensures consistent database configuration across all connections

**Connection Disposal:**
```csharp
// Pattern used throughout codebase:
var context = await DbContextFactory.CreateDbContextAsync(cancellationToken);
await using (context.ConfigureAwait(false))
{
    // Use context
}
// Context automatically disposed, connection returned to pool
```

### Pool Clearing

**Manual Pool Clearing (SqliteDatabaseProvider.cs:132):**
```csharp
SqliteConnection.ClearAllPools();
```

**Called During:**
1. Application shutdown (`RunShutdownTask`)
2. Before backup restore (`RestoreBackupFast`)
3. To release all database file locks

---

## SQLite Configuration & Pragmas

### Pragma Injection System

**PragmaConnectionInterceptor** applies pragmas on every connection open event.

**File:** `PragmaConnectionInterceptor.cs`

**Architecture:**
```csharp
public override async Task ConnectionOpenedAsync(
    DbConnection connection,
    ConnectionEndEventData eventData,
    CancellationToken cancellationToken = default)
{
    var command = connection.CreateCommand();
    await using (command.ConfigureAwait(false))
    {
        command.CommandText = InitialCommand;  // Pre-built pragma commands
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
```

### Default Pragma Configuration

**Applied on Every Connection:**

| Pragma | Default Value | Purpose | Configurable |
|--------|--------------|---------|--------------|
| `cache_size` | (not set) | Number of pages in memory cache | Yes (`cacheSize`) |
| `locking_mode` | `NORMAL` | Database locking strategy | Yes (`lockingmode`) |
| `journal_size_limit` | `134,217,728` (128 MB) | Maximum WAL journal size | Yes (`journalsizelimit`) |
| `synchronous` | `1` (NORMAL) | Disk sync frequency | Yes (`syncmode`) |
| `temp_store` | `2` (MEMORY) | Temp table storage location | Yes (`tempstoremode`) |

### Pragma Details

**`cache_size`:**
- Controls in-memory page cache
- Positive value = pages, Negative value = KB
- Larger values improve read performance
- Default: SQLite default (~2000 pages or ~2MB)

**`locking_mode`:**
- `NORMAL` (default): Release locks after each transaction
- `EXCLUSIVE`: Hold exclusive lock on database file
- Default `NORMAL` allows concurrent access

**`journal_size_limit`:**
- Limits Write-Ahead Log (WAL) file size
- Default: 128 MB (134,217,728 bytes)
- WAL auto-checkpoints when limit reached

**`synchronous`:**
- `0` (OFF): No sync, fastest but risky
- `1` (NORMAL): Sync at critical moments (default)
- `2` (FULL): Sync after every write
- `3` (EXTRA): Maximum durability
- Default `NORMAL` balances safety and performance

**`temp_store`:**
- `0` (DEFAULT): SQLite decides
- `1` (FILE): Temp tables on disk
- `2` (MEMORY): Temp tables in RAM (default)
- Default `MEMORY` improves temp operation performance

### Custom Pragmas

**Configuration Format:**
```json
{
  "database": {
    "DatabaseType": "Jellyfin-SQLite",
    "CustomProviderOptions": {
      "Options": [
        {"Key": "#PRAGMA:journal_mode", "Value": "WAL"},
        {"Key": "#PRAGMA:wal_autocheckpoint", "Value": "1000"}
      ]
    }
  }
}
```

**Prefix:** `#PRAGMA:` signals custom pragma directive
**Example:** `#PRAGMA:busy_timeout` → `PRAGMA busy_timeout=5000;`

### Database File Location

**Path:** `{ApplicationDataPath}/jellyfin.db`

**Associated Files:**
- `jellyfin.db` - Main database file
- `jellyfin.db-wal` - Write-Ahead Log (WAL mode)
- `jellyfin.db-shm` - Shared memory file (WAL mode)

---

## Concurrency Strategies

Jellyfin provides three configurable concurrency strategies to handle simultaneous database access.

### Configuration

**File:** `ServiceCollectionExtensions.cs:126-137`

```csharp
switch (efCoreConfiguration.LockingBehavior)
{
    case DatabaseLockingBehaviorTypes.NoLock:
        serviceCollection.AddSingleton<IEntityFrameworkCoreLockingBehavior, NoLockBehavior>();
        break;
    case DatabaseLockingBehaviorTypes.Pessimistic:
        serviceCollection.AddSingleton<IEntityFrameworkCoreLockingBehavior, PessimisticLockBehavior>();
        break;
    case DatabaseLockingBehaviorTypes.Optimistic:
        serviceCollection.AddSingleton<IEntityFrameworkCoreLockingBehavior, OptimisticLockBehavior>();
        break;
}
```

---

### 1. NoLock Behavior (Default)

**File:** `NoLockBehavior.cs`

**Strategy:** Relies entirely on SQLite's built-in locking and WAL mode for concurrency.

**Implementation:**
```csharp
public async Task OnSaveChangesAsync(JellyfinDbContext context, Func<Task> saveChanges)
{
    await saveChanges().ConfigureAwait(false);  // No additional locking
}
```

**Characteristics:**
- ✅ Lowest overhead
- ✅ Best performance for single-user or light concurrent access
- ✅ WAL mode allows concurrent readers with one writer
- ⚠️ May encounter "database is locked" errors under heavy concurrent writes
- ⚠️ Application must handle retry logic if needed

**Use Cases:**
- Single-user installations
- Light concurrent access
- Default configuration for most deployments

**SQLite WAL Mode:**
- Multiple readers don't block each other
- One writer can proceed while readers are active
- Readers see consistent snapshot

---

### 2. Pessimistic Lock Behavior

**File:** `PessimisticLockBehavior.cs`

**Strategy:** Application-level read/write locking that blocks all operations during writes.

**Architecture:**
```csharp
private static ReaderWriterLockSlim DatabaseLock { get; } =
    new(LockRecursionPolicy.SupportsRecursion);
```

**Locking Rules:**

| Operation | Lock Type | Behavior |
|-----------|-----------|----------|
| Read (SELECT, Scalar) | Read Lock | Multiple readers allowed |
| Write (INSERT, UPDATE, DELETE) | Write Lock | Exclusive, blocks all |
| SaveChanges | Write Lock | Exclusive, blocks all |
| Transaction Start | Write Lock | Exclusive for entire transaction |

**Implementation via Interceptors:**

1. **CommandLockingInterceptor** - Wraps individual commands:
```csharp
// Read operations
public override InterceptionResult<DbDataReader> ReaderExecuting(...)
{
    using (DbLock.EnterRead(_logger))
    {
        return InterceptionResult<DbDataReader>.SuppressWithResult(command.ExecuteReader());
    }
}

// Write operations
public override InterceptionResult<int> NonQueryExecuting(...)
{
    using (DbLock.EnterWrite(_logger, command))
    {
        return InterceptionResult<int>.SuppressWithResult(command.ExecuteNonQuery());
    }
}
```

2. **TransactionLockingInterceptor** - Holds write lock for entire transaction:
```csharp
public override InterceptionResult<DbTransaction> TransactionStarting(...)
{
    DbLock.BeginWriteLock(_logger);  // Held until commit/rollback
    return base.TransactionStarting(connection, eventData, result);
}

public override void TransactionCommitted(...)
{
    DbLock.EndWriteLock(_logger);  // Released after commit
    base.TransactionCommitted(transaction, eventData);
}
```

**Deadlock Prevention:**
- `LockRecursionPolicy.SupportsRecursion` allows re-entrant locks
- Check if write lock already held before acquiring

**Congestion Detection:**
```csharp
if (!DatabaseLock.TryEnterWriteLock(TimeSpan.FromMilliseconds(1000)))
{
    // Log congestion details
    logger.LogInformation("Query congestion detected: '{Id}' since '{Date}'", ...);
    DatabaseLock.EnterWriteLock();  // Block until available
    logger.LogInformation("Query congestion cleared: '{Id}' for '{Duration}'", ...);
}
```

**Characteristics:**
- ✅ Eliminates "database is locked" errors
- ✅ Predictable behavior under high concurrency
- ✅ Mimics old SqliteRepository behavior
- ⚠️ Reduced concurrency (writes block everything)
- ⚠️ Potential throughput reduction under heavy load

**Use Cases:**
- Multi-user deployments with frequent concurrent writes
- Environments requiring strict consistency
- Migration from legacy locking behavior

---

### 3. Optimistic Lock Behavior

**File:** `OptimisticLockBehavior.cs`

**Strategy:** Retry-based approach using Polly for handling transient lock failures.

**Architecture:**
```csharp
private readonly Policy _writePolicy;
private readonly AsyncPolicy _writeAsyncPolicy;
```

**Retry Configuration:**

**Backoff Schedule (15 retries):**
```csharp
TimeSpan[] sleepDurations = [
    50ms, 50ms, 50ms, 50ms,          // Quick retries
    250ms, 250ms, 250ms,              // Medium delays
    150ms, 150ms, 150ms,              // Variable timing
    500ms, 150ms, 500ms, 150ms,      // Longer delays
    3000ms                            // Final long delay
];
```

**Total Maximum Wait:** ~6.5 seconds across 15 retries

**Jitter Applied:**
```csharp
backoff + TimeSpan.FromMilliseconds(RandomNumberGenerator.GetInt32(0, (int)(backoff.TotalMilliseconds * .5)))
```
- Adds 0-50% random jitter to each backoff
- Prevents thundering herd problem

**Error Detection:**
```csharp
_writePolicy = Policy
    .HandleInner<Exception>(e =>
        e.Message.Contains("database is locked", StringComparison.InvariantCultureIgnoreCase) ||
        e.Message.Contains("database table is locked", StringComparison.InvariantCultureIgnoreCase))
    .WaitAndRetry(sleepDurations.Length, backoffProvider, RetryHandle);
```

**Retry Handler:**
```csharp
void RetryHandle(Exception exception, TimeSpan timespan, int retryNo, Context context)
{
    if (retryNo < sleepDurations.Length)
    {
        _logger.LogWarning("Operation failed retry {RetryNo}", retryNo);
    }
    else
    {
        _logger.LogError(exception, "Operation failed retry {RetryNo}", retryNo);
    }
}
```

**Applied To:**
- All command executions (NonQuery, Scalar, Reader)
- Transaction start operations
- SaveChanges operations

**Implementation via Interceptors:**

1. **RetryInterceptor** - Wraps individual commands:
```csharp
public override async ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(...)
{
    return InterceptionResult<int>.SuppressWithResult(
        await _asyncRetryPolicy.ExecuteAsync(
            async () => await command.ExecuteNonQueryAsync(cancellationToken)
        )
    );
}
```

2. **TransactionLockingInterceptor** - Retries transaction start:
```csharp
public override async ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(...)
{
    return InterceptionResult<DbTransaction>.SuppressWithResult(
        await _asyncRetryPolicy.ExecuteAsync(
            async () => await connection.BeginTransactionAsync(eventData.IsolationLevel, cancellationToken)
        )
    );
}
```

**Characteristics:**
- ✅ Good balance between concurrency and reliability
- ✅ No explicit application locks (uses SQLite's locking)
- ✅ Automatically handles transient failures
- ✅ Better throughput than pessimistic under moderate load
- ⚠️ Operations may take longer due to retries
- ⚠️ Still possible to fail after all retries exhausted

**Use Cases:**
- Multi-user deployments with moderate concurrent writes
- Balance between performance and reliability
- Environments with occasional but not constant lock contention

---

### Strategy Comparison

| Feature | NoLock | Pessimistic | Optimistic |
|---------|--------|-------------|------------|
| **Concurrency** | High (WAL) | Low (blocked) | High (retry) |
| **Throughput** | Best | Moderate | Good |
| **Latency** | Lowest | Variable (blocking) | Variable (retries) |
| **Lock Errors** | Possible | None | Handled (retry) |
| **Resource Usage** | Minimal | Lock overhead | Retry overhead |
| **Predictability** | Low | High | Moderate |
| **Best For** | Single user | Heavy writes | Moderate writes |

---

## Transaction Management

### Transaction Handling

**EFCore manages transactions automatically:**
- `SaveChanges()` / `SaveChangesAsync()` wrap operations in transaction
- Explicit transactions via `context.Database.BeginTransaction()`

### SaveChanges Integration

**File:** `JellyfinDbContext.cs:256-276`

```csharp
public override async Task<int> SaveChangesAsync(
    bool acceptAllChangesOnSuccess,
    CancellationToken cancellationToken = default)
{
    HandleConcurrencyToken();  // Update concurrency tokens

    try
    {
        var result = -1;
        await entityFrameworkCoreLocking.OnSaveChangesAsync(this, async () =>
        {
            result = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
        });
        return result;
    }
    catch (Exception e)
    {
        logger.LogError(e, "Error trying to save changes.");
        throw;
    }
}
```

**Flow:**
1. Update concurrency tokens on modified entities
2. Pass operation to locking behavior
3. Locking behavior applies strategy (NoLock/Pessimistic/Optimistic)
4. Execute base SaveChanges
5. Return result or propagate exception

### Concurrency Tokens

**Interface:** `IHasConcurrencyToken`

**Purpose:** Optimistic concurrency control at entity level

**Implementation (JellyfinDbContext.cs:299-308):**
```csharp
private void HandleConcurrencyToken()
{
    foreach (var saveEntity in ChangeTracker.Entries()
                 .Where(e => e.State == EntityState.Modified)
                 .Select(entry => entry.Entity)
                 .OfType<IHasConcurrencyToken>())
    {
        saveEntity.OnSavingChanges();  // Increment token
    }
}
```

**Entity-Level Protection:**
- Prevents lost updates when multiple contexts modify same entity
- EFCore throws `DbUpdateConcurrencyException` if token mismatch
- Independent of application-level locking strategy

### Isolation Levels

**SQLite Support:**
- SQLite doesn't fully support standard isolation levels
- Effectively operates at `Serializable` with WAL mode
- Transactions see snapshot of database at transaction start

---

## Performance & Optimization

### Scheduled Optimization

**File:** `SqliteDatabaseProvider.cs:98-109`

**Operation:** `RunScheduledOptimisation`

**Process:**
```csharp
await context.Database.ExecuteSqlRawAsync("PRAGMA wal_checkpoint(TRUNCATE)", cancellationToken);
await context.Database.ExecuteSqlRawAsync("PRAGMA optimize", cancellationToken);
await context.Database.ExecuteSqlRawAsync("VACUUM", cancellationToken);
await context.Database.ExecuteSqlRawAsync("PRAGMA wal_checkpoint(TRUNCATE)", cancellationToken);
```

**Steps:**

1. **`PRAGMA wal_checkpoint(TRUNCATE)`**
   - Checkpoints WAL file to main database
   - Truncates WAL file to zero bytes
   - Frees disk space from WAL

2. **`PRAGMA optimize`**
   - Gathers statistics on tables and indexes
   - Updates query planner information
   - Improves query performance

3. **`VACUUM`**
   - Rebuilds database file
   - Reclaims unused space from deletions
   - Defragments database
   - **Warning:** Locks database during operation

4. **`PRAGMA wal_checkpoint(TRUNCATE)`** (again)
   - Final checkpoint after vacuum
   - Ensures WAL is clean

**Frequency:** Should be scheduled during low-usage periods (maintenance window)

**Impact:**
- `VACUUM` locks database (blocks all access)
- Can be time-consuming on large databases
- Significant I/O activity

### Shutdown Optimization

**File:** `SqliteDatabaseProvider.cs:118-133`

**Operation:** `RunShutdownTask`

**Process:**
```csharp
var context = await DbContextFactory.CreateDbContextAsync(cancellationToken);
await using (context.ConfigureAwait(false))
{
    await context.Database.ExecuteSqlRawAsync("PRAGMA optimize", cancellationToken);
}
SqliteConnection.ClearAllPools();
```

**Purpose:**
- Optimize query planner before shutdown
- Clear all connection pools
- Release database file locks
- Clean shutdown state

### Performance Tuning Recommendations

**For Read-Heavy Workloads:**
```json
{
  "cacheSize": -64000,  // 64 MB cache (negative = KB)
  "tempstoremode": 2,    // Memory temp storage
  "#PRAGMA:mmap_size": "268435456"  // 256 MB memory-mapped I/O
}
```

**For Write-Heavy Workloads:**
```json
{
  "syncmode": 1,         // NORMAL sync (balance safety/speed)
  "journalsizelimit": 268435456,  // 256 MB WAL limit
  "#PRAGMA:wal_autocheckpoint": "1000"  // Checkpoint every 1000 pages
}
```

**For Maximum Performance (Development Only):**
```json
{
  "syncmode": 0,         // OFF - DANGEROUS in production
  "tempstoremode": 2,    // Memory temp storage
  "cacheSize": -128000   // 128 MB cache
}
```
**⚠️ Warning:** `syncmode=0` risks database corruption on power loss

---

## Backup & Restore

### Fast Backup (File Copy)

**File:** `SqliteDatabaseProvider.cs:142-152`

**Method:** `MigrationBackupFast`

**Process:**
```csharp
var key = DateTime.UtcNow.ToString("yyyyMMddhhmmss", CultureInfo.InvariantCulture);
var path = Path.Combine(_applicationPaths.DataPath, "jellyfin.db");
var backupFile = Path.Combine(_applicationPaths.DataPath, "SQLiteBackups", $"{key}_jellyfin.db");
Directory.CreateDirectory(backupFolder);
File.Copy(path, backupFile);
return key;
```

**Backup Location:** `{DataPath}/SQLiteBackups/{timestamp}_jellyfin.db`

**Key Format:** `yyyyMMddhhmmss` (e.g., `20250103143022`)

**Characteristics:**
- Fast (simple file copy)
- No SQL operations
- Captures database at moment of copy
- WAL file not backed up separately (data may be in WAL)

**⚠️ Consideration:** For consistent backup, ensure WAL checkpoint first:
```sql
PRAGMA wal_checkpoint(TRUNCATE);
```

### Fast Restore

**File:** `SqliteDatabaseProvider.cs:155-170`

**Method:** `RestoreBackupFast`

**Process:**
```csharp
SqliteConnection.ClearAllPools();  // Critical: Release locks
var path = Path.Combine(_applicationPaths.DataPath, "jellyfin.db");
var backupFile = Path.Combine(_applicationPaths.DataPath, "SQLiteBackups", $"{key}_jellyfin.db");
File.Copy(backupFile, path, true);  // Overwrite
```

**Critical Step:** `ClearAllPools()` before restore
- Closes all open connections
- Releases file locks
- Allows file overwrite

**Post-Restore:** Application must reconnect to database

### Backup Management

**Delete Backup:**
```csharp
var backupFile = Path.Combine(_applicationPaths.DataPath, "SQLiteBackups", $"{key}_jellyfin.db");
File.Delete(backupFile);
```

**Best Practices:**
1. Checkpoint WAL before backup for consistency
2. Verify backup file after creation
3. Store backups on separate storage if possible
4. Implement backup rotation (delete old backups)
5. Test restore procedure regularly

---

## Configuration Reference

### Database Configuration Options

**Configuration Key:** `"database"`

**Schema:**
```csharp
{
  "DatabaseType": "Jellyfin-SQLite",
  "LockingBehavior": "NoLock",  // NoLock | Pessimistic | Optimistic
  "CustomProviderOptions": {
    "Options": [
      {"Key": "pooling", "Value": "true"},
      {"Key": "cache", "Value": "Default"},  // Default | Private | Shared
      {"Key": "cacheSize", "Value": "2000"},  // Pages (positive) or KB (negative)
      {"Key": "lockingmode", "Value": "NORMAL"},  // NORMAL | EXCLUSIVE
      {"Key": "journalsizelimit", "Value": "134217728"},  // Bytes
      {"Key": "tempstoremode", "Value": "2"},  // 0=DEFAULT, 1=FILE, 2=MEMORY
      {"Key": "syncmode", "Value": "1"},  // 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA
      {"Key": "EnableSensitiveDataLogging", "Value": "false"},
      {"Key": "#PRAGMA:journal_mode", "Value": "WAL"},
      {"Key": "#PRAGMA:busy_timeout", "Value": "5000"}
    ]
  }
}
```

### Command-Line Override

```bash
jellyfin --migration-provider Jellyfin-SQLite
```

### Custom Provider (Plugin)

```json
{
  "DatabaseType": "PLUGIN_PROVIDER",
  "CustomProviderOptions": {
    "PluginName": "Jellyfin.Plugin.PostgreSQL",
    "PluginAssembly": "Jellyfin.Plugin.PostgreSQL",
    "ConnectionString": "Host=localhost;Database=jellyfin;...",
    "Options": [...]
  }
}
```

### Environment-Specific Recommendations

**Single User / Home Server:**
```json
{
  "DatabaseType": "Jellyfin-SQLite",
  "LockingBehavior": "NoLock"
}
```

**Small Multi-User (2-5 concurrent):**
```json
{
  "DatabaseType": "Jellyfin-SQLite",
  "LockingBehavior": "Optimistic",
  "CustomProviderOptions": {
    "Options": [
      {"Key": "cacheSize", "Value": "-32000"}  // 32 MB cache
    ]
  }
}
```

**Larger Multi-User (5+ concurrent):**
```json
{
  "DatabaseType": "Jellyfin-SQLite",
  "LockingBehavior": "Pessimistic",
  "CustomProviderOptions": {
    "Options": [
      {"Key": "cacheSize", "Value": "-65536"},  // 64 MB cache
      {"Key": "journalsizelimit", "Value": "268435456"}  // 256 MB WAL
    ]
  }
}
```

**High-Performance / SSD:**
```json
{
  "DatabaseType": "Jellyfin-SQLite",
  "LockingBehavior": "Optimistic",
  "CustomProviderOptions": {
    "Options": [
      {"Key": "cacheSize", "Value": "-131072"},  // 128 MB cache
      {"Key": "syncmode", "Value": "1"},  // NORMAL sync
      {"Key": "#PRAGMA:mmap_size", "Value": "268435456"}  // 256 MB mmap
    ]
  }
}
```

---

## Key Files Reference

| Component | File Path | Purpose |
|-----------|-----------|---------|
| SQLite Provider | `src/Jellyfin.Database/Jellyfin.Database.Providers.Sqlite/SqliteDatabaseProvider.cs` | SQLite initialization & operations |
| Pragma Interceptor | `src/Jellyfin.Database/Jellyfin.Database.Providers.Sqlite/PragmaConnectionInterceptor.cs` | Connection-level pragma injection |
| DB Context | `src/Jellyfin.Database/Jellyfin.Database.Implementations/JellyfinDbContext.cs` | Main EFCore context |
| Service Registration | `Jellyfin.Server.Implementations/Extensions/ServiceCollectionExtensions.cs` | DI setup & provider selection |
| NoLock Behavior | `src/Jellyfin.Database/Jellyfin.Database.Implementations/Locking/NoLockBehavior.cs` | No-op locking strategy |
| Pessimistic Locking | `src/Jellyfin.Database/Jellyfin.Database.Implementations/Locking/PessimisticLockBehavior.cs` | Reader/writer lock implementation |
| Optimistic Locking | `src/Jellyfin.Database/Jellyfin.Database.Implementations/Locking/OptimisticLockBehavior.cs` | Retry-based locking with Polly |
| Provider Interface | `src/Jellyfin.Database/Jellyfin.Database.Implementations/IJellyfinDatabaseProvider.cs` | Provider abstraction |

---

## Summary

Jellyfin's database architecture provides:

1. **Flexible Provider System** - Pluggable backends (SQLite default, custom via plugins)
2. **Efficient Pooling** - EFCore pooled factory with configurable connection settings
3. **Sophisticated Concurrency** - Three strategies (NoLock, Pessimistic, Optimistic) for different deployment scenarios
4. **Automatic Configuration** - Pragma injection ensures consistent SQLite behavior
5. **Performance Tools** - Scheduled optimization, shutdown cleanup, tunable parameters
6. **Operational Safety** - Fast backup/restore, connection pool management, error handling

The architecture balances **performance, reliability, and flexibility** while maintaining clean separation between database operations and business logic through EFCore abstraction.
