using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
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

        private const int BLOCK_BATCH = 50;
        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(5);

        public ExportingRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var tasks = Parameterization.Activities.Values
                .GroupBy(a => a.Source.GetTableIdentity().ClusterUri)
                .Select(g => Task.Run(() => RunActivitiesAsync(
                    g.Key,
                    g.Select(a => a.ActivityName).ToImmutableArray(),
                    ct)))
                .ToImmutableList();

            await Task.WhenAll(tasks);
        }

        private async Task RunActivitiesAsync(
            Uri sourceClusterUri,
            IImmutableList<string> activityNames,
            CancellationToken ct)
        {
            var capacityCache =
                new CapacityCache(DateTime.Now.Subtract(CAPACITY_REFRESH_PERIOD), 0);

            while (!AreActivitiesCompleted(activityNames))
            {
                //  Ensures capacity of source cluster
                capacityCache = capacityCache.CachedTime < DateTime.Now
                    ? capacityCache
                    : new CapacityCache(
                        DateTime.Now,
                        await FetchCapacityAsync(sourceClusterUri, ct));

                var plannedBlocks = FetchPlannedBlocks(activityNames, capacityCache.CachedCapacity);

                if (plannedBlocks.Any())
                {
                    var iterationKeys = plannedBlocks
                        .Select(b => b.BlockKey.ToIterationKey());
                    var iterationRecordMap = Database.Iterations.Query()
                        .Where(pf => pf.In(i => i.IterationKey, iterationKeys))
                        .ToImmutableDictionary(i => i.IterationKey);
                    var startExportTasks = plannedBlocks
                        .Select(b => Task.Run(() => StartExportAsync(
                            b,
                            iterationRecordMap[b.BlockKey.ToIterationKey()],
                            ct)))
                        .ToImmutableArray();

                    await Task.WhenAll(startExportTasks);
                }
                else
                {
                    await SleepAsync(ct);
                }
            }
        }

        private IEnumerable<BlockRecord> FetchPlannedBlocks(
            IEnumerable<string> activityNames,
            int cachedCapacity)
        {
            //  Fetch a batch of blocks ready to export
            var plannedBlocks = Database.Blocks.Query()
                .Where(pf => pf.In(b => b.BlockKey.ActivityName, activityNames))
                .Where(pf => pf.Equal(b => b.State, BlockState.Planned))
                .OrderBy(b => b.BlockKey.ActivityName)
                .ThenBy(b => b.BlockKey.IterationId)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(BLOCK_BATCH)
                .ToImmutableArray();

            if (plannedBlocks.Any())
            {
                //  Fetch all blocks being in 'exporting' state
                var exportingCount = (int)Database.Blocks.Query()
                    .Where(pf => pf.In(b => b.BlockKey.ActivityName, activityNames))
                    .Where(pf => pf.Equal(b => b.State, BlockState.Exporting))
                    .Count();

                //  Cap the blocks with available capacity
                return plannedBlocks
                    .Take(Math.Max(0, cachedCapacity - exportingCount));
            }
            else
            {
                return plannedBlocks;
            }
        }

        private async Task StartExportAsync(
            BlockRecord blockRecord,
            IterationRecord iterationRecord,
            CancellationToken ct)
        {
            var activityParam = Parameterization.Activities[blockRecord.BlockKey.ActivityName];
            var sourceTable = activityParam.Source.GetTableIdentity();
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

            using (var tx = Database.Database.CreateTransaction())
            {
                Database.Blocks.Query()
                    .Where(pf => pf.MatchKeys(
                        newBlockRecord,
                        b => b.BlockKey.ActivityName,
                        b => b.BlockKey.IterationId,
                        b => b.BlockKey.BlockId))
                    .Delete();
                Database.Blocks.AppendRecord(newBlockRecord);
            }
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