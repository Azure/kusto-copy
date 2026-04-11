using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class ExportingRunner : RunnerBase
    {
        #region Inner Types
        private record CapacityCache(DateTime CachedTime, int CachedCapacity);
        #endregion

        private const int BLOCK_BATCH = 10;
        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(2);

        public ExportingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(10))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var cacheMap = new Dictionary<Uri, CapacityCache>();

            while (!AreActivitiesCompleted())
            {
                var activityNames = Database.Activities.Query()
                    .Where(pf => pf.NotEqual(a => a.State, ActivityState.Completed))
                    .Select(a => a.ActivityName)
                    .ToHashSet();
                var groupings = Parameterization.Activities
                    .Where(a => activityNames.Contains(a.ActivityName))
                    .GroupBy(a => a.GetSourceTableIdentity().ClusterUri)
                    .Select(g => new
                    {
                        SourceClusterUri = g.Key,
                        ActivityNames = g
                        .Select(a => a.ActivityName)
                        .OrderBy(n => n)
                        .ToArray()
                    })
                    .ToDictionary(g => g.SourceClusterUri);

                //  Ensures cached capacity of source cluster
                foreach (var grouping in groupings.Values)
                {
                    if (!cacheMap.ContainsKey(grouping.SourceClusterUri)
                        || cacheMap[grouping.SourceClusterUri].CachedTime
                        + CAPACITY_REFRESH_PERIOD < DateTime.Now)
                    {
                        cacheMap[grouping.SourceClusterUri] = new CapacityCache(
                            DateTime.Now,
                            await FetchCapacityAsync(grouping.SourceClusterUri, ct));
                    }
                }
                //  Retired unused capacity
                foreach (var sourceClusterUri in cacheMap.Keys)
                {
                    if (!groupings.ContainsKey(sourceClusterUri))
                    {
                        cacheMap.Remove(sourceClusterUri);
                    }
                }

                var tasks = groupings
                    .Values
                    .Select(g => Task.Run(() => RunClusterExportAsync(
                        g.SourceClusterUri,
                        cacheMap[g.SourceClusterUri].CachedCapacity,
                        g.ActivityNames,
                        ct)))
                    .ToArray();

                await Task.WhenAll(tasks);
                await SleepAsync(ct);
            }
        }

        private async Task RunClusterExportAsync(
            Uri sourceClusterUri,
            int capacity,
            string[] activityNames,
            CancellationToken ct)
        {
            while (await ExportBatchAsync(
                sourceClusterUri,
                capacity,
                activityNames,
                ct))
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private async Task<bool> ExportBatchAsync(
            Uri sourceClusterUri,
            int capacity,
            string[] activityNames,
            CancellationToken ct)
        {
            var plannedBlocks = FetchPlannedBlocks(activityNames, capacity);

            if (plannedBlocks.Count() > 0)
            {
                var iterationKey = plannedBlocks.First().BlockKey.IterationKey;
                var iteration = Database.Iterations.Query()
                    .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                    .First();
                var startExportTasks = plannedBlocks
                    .Select(b => Task.Run(() => StartExportAsync(b, iteration, ct)))
                    .ToImmutableArray();

                await Task.WhenAll(startExportTasks);

                return true;
            }

            return false;
        }

        private IEnumerable<BlockRecord> FetchPlannedBlocks(
            IEnumerable<string> activityNames,
            int capacity)
        {   //  The first (by priority) block will determine the activity and iteration
            //  we'll work on
            var firstPlannedBlock = Database.Blocks.Query()
                .Where(pf => pf.In(b => b.BlockKey.IterationKey.ActivityName, activityNames))
                .Where(pf => pf.Equal(b => b.State, BlockState.Planned))
                .OrderBy(b => b.BlockKey.IterationKey.IterationId)
                .ThenBy(b => b.BlockKey.IterationKey.ActivityName)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(1)
                .FirstOrDefault();

            if (firstPlannedBlock != null)
            {
                //  Fetch all blocks being in 'exporting' state
                var exportingCount = (int)Database.Blocks.Query()
                    .Where(pf => pf.In(b => b.BlockKey.IterationKey.ActivityName, activityNames))
                    .Where(pf => pf.Equal(b => b.State, BlockState.Exporting))
                    .Count();
                //  Cap the blocks with override capacity
                var cappedCachedCapacity = Parameterization.ExportCount == null
                    ? capacity
                    : Math.Min(capacity, Parameterization.ExportCount.Value);
                //  Cap the blocks with available capacity
                var maxExporting =
                    Math.Min(BLOCK_BATCH, Math.Max(0, cappedCachedCapacity - exportingCount));
                var plannedBlocks = Database.Blocks.Query()
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.IterationKey,
                        firstPlannedBlock.BlockKey.IterationKey))
                    .Where(pf => pf.Equal(b => b.State, BlockState.Planned))
                    .OrderBy(b => b.BlockKey.BlockId)
                    .Take(maxExporting)
                    .ToImmutableArray();

                return plannedBlocks;
            }
            else
            {
                return Array.Empty<BlockRecord>();
            }
        }

        private async Task StartExportAsync(
            BlockRecord blockRecord,
            IterationRecord iterationRecord,
            CancellationToken ct)
        {
            var activityParam =
                Parameterization.GetActivity(blockRecord.BlockKey.IterationKey.ActivityName);
            var sourceTable = activityParam.GetSourceTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                sourceTable.ClusterUri,
                sourceTable.DatabaseName);
            var writableUris = await StagingBlobUriProvider.GetWritableFolderUrisAsync(
                blockRecord.BlockKey,
                ct);
            var operationId = await dbClient.ExportBlockAsync(
                new KustoPriority(blockRecord.BlockKey),
                writableUris,
                sourceTable.TableName,
                activityParam.KqlQuery,
                iterationRecord.CursorStart,
                iterationRecord.CursorEnd,
                blockRecord.IngestionTimeStart,
                blockRecord.IngestionTimeEnd,
                ct);
            var newBlockRecord = blockRecord with
            {
                State = BlockState.Exporting,
                ExportOperationId = operationId
            };

            Database.Blocks.UpdateRecord(blockRecord, newBlockRecord);
        }

        private async Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var capacity = await dbCommandClient.ShowExportCapacityAsync(
                KustoPriority.HighestPriority,
                ct);

            return capacity;
        }
    }
}