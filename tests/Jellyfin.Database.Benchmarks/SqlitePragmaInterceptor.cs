using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Jellyfin.Database.Benchmarks;

/// <summary>
/// Interceptor that applies SQLite pragmas to every connection when opened.
/// </summary>
public class SqlitePragmaInterceptor : DbConnectionInterceptor
{
    private readonly int _busyTimeout;

    public SqlitePragmaInterceptor(int busyTimeout = 5000)
    {
        _busyTimeout = busyTimeout;
    }

    public override void ConnectionOpened(DbConnection connection, ConnectionEndEventData eventData)
    {
        ApplyPragmas(connection);
        base.ConnectionOpened(connection, eventData);
    }

    public override async Task ConnectionOpenedAsync(
        DbConnection connection,
        ConnectionEndEventData eventData,
        CancellationToken cancellationToken = default)
    {
        await ApplyPragmasAsync(connection, cancellationToken);
        await base.ConnectionOpenedAsync(connection, eventData, cancellationToken);
    }

    private void ApplyPragmas(DbConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA busy_timeout={_busyTimeout};";
        command.ExecuteNonQuery();
    }

    private async Task ApplyPragmasAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA busy_timeout={_busyTimeout};";
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Updates the busy_timeout value dynamically.
    /// Note: This only affects NEW connections. Existing pooled connections retain their old value.
    /// </summary>
    public void UpdateBusyTimeout(int newTimeout)
    {
        // Not implemented - would require tracking and updating all open connections
        throw new NotImplementedException(
            "Cannot dynamically update busy_timeout on pooled connections. " +
            "Create a new service provider with the desired timeout instead.");
    }
}
