using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Emby.Server.Implementations.Data;
using Jellyfin.Server.Implementations;
using MediaBrowser.Controller;
using MediaBrowser.Model.Globalization;
using MediaBrowser.Model.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Emby.Server.Implementations.ScheduledTasks.Tasks
{
    /// <summary>
    /// Optimizes Jellyfin's database by issuing a VACUUM command.
    /// </summary>
    public class OptimizeDatabaseExtendedTask : IScheduledTask, IConfigurableScheduledTask
    {
        private const string DbFilename = "library.db";
        private readonly ILogger<OptimizeDatabaseExtendedTask> _logger;
        private readonly ILocalizationManager _localization;
        private readonly IDbContextFactory<JellyfinDbContext> _provider;
        private readonly IServerApplicationPaths _applicationPaths;

        /// <summary>
        /// Initializes a new instance of the <see cref="OptimizeDatabaseExtendedTask" /> class.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <param name="localization">The localization manager.</param>
        /// <param name="provider">The jellyfin DB context provider.</param>
        /// <param name="applicationPaths">Instance of the <see cref="IServerApplicationPaths"/> interface.</param>
        public OptimizeDatabaseExtendedTask(
            ILogger<OptimizeDatabaseExtendedTask> logger,
            ILocalizationManager localization,
            IServerApplicationPaths applicationPaths,
            IDbContextFactory<JellyfinDbContext> provider)
        {
            _logger = logger;
            _localization = localization;
            _provider = provider;
            _applicationPaths = applicationPaths;
        }

        /// <inheritdoc />
        // public string Name => _localization.GetLocalizedString("TaskOptimizeDatabase");
        public string Name => "Optimise Database - Extended";

        /// <inheritdoc />
        // public string Description => _localization.GetLocalizedString("TaskOptimizeDatabaseDescription");
        public string Description => "Compacts database and truncates free space. Running this task after adding or deleting library folders or doing other changes that imply significant database modifications might improve performance.";

        /// <inheritdoc />
        public string Category => _localization.GetLocalizedString("TasksMaintenanceCategory");

        /// <inheritdoc />
        public string Key => "OptimizeDatabaseExtendedTask";

        /// <inheritdoc />
        public bool IsHidden => false;

        /// <inheritdoc />
        public bool IsEnabled => true;

        /// <inheritdoc />
        public bool IsLogged => true;

        /// <summary>
        /// Creates the triggers that define when the task will run.
        /// </summary>
        /// <returns>IEnumerable{BaseTaskTrigger}.</returns>
        public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
        {
            return new[]
            {
                // Every so often
                new TaskTriggerInfo { Type = TaskTriggerInfo.TriggerInterval, IntervalTicks = TimeSpan.FromDays(7).Ticks }
            };
        }

        /// <inheritdoc />
        public async Task ExecuteAsync(IProgress<double> progress, CancellationToken cancellationToken)
        {
            try
            {
                var dataPath = _applicationPaths.DataPath;
                var dbPath = Path.Combine(dataPath, DbFilename);
                using (var connection = new SqliteConnection($"Filename={dbPath};Pooling=False"))
                {
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                    _logger.LogInformation("Optimizing DB: library.db");

                    connection.Execute("PRAGMA integrity_check(1)");
                    connection.Execute("PRAGMA foreign_key_check");
                    connection.Execute("VACUUM");
                    connection.Execute("PRAGMA analysis_limit=0; ANALYZE; PRAGMA optimize");
                    connection.Execute("REINDEX");
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error Optimizing DB: library.db");
            }

            try
            {
                var context = await _provider.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
                await using (context.ConfigureAwait(false))
                {
                    if (context.Database.IsSqlite())
                    {
                        _logger.LogInformation("Optimizing DB: jellyfin.db");

                        await context.Database.ExecuteSqlRawAsync("PRAGMA integrity_check(1)", cancellationToken).ConfigureAwait(false);
                        await context.Database.ExecuteSqlRawAsync("PRAGMA foreign_key_check", cancellationToken).ConfigureAwait(false);
                        await context.Database.ExecuteSqlRawAsync("VACUUM", cancellationToken).ConfigureAwait(false);
                        await context.Database.ExecuteSqlRawAsync("PRAGMA analysis_limit=0; ANALYZE; PRAGMA optimize", cancellationToken).ConfigureAwait(false);
                        await context.Database.ExecuteSqlRawAsync("REINDEX", cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.LogInformation("Database is not SQLite, skipping optimization.");
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error Optimizing DB: jellyfin.db");
            }
        }
    }
}
