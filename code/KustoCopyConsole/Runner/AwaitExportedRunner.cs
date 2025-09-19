using Azure.Core;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class AwaitExportedRunner : RunnerBase
    {
        private const int MAX_OPERATIONS = 200;
        private static readonly IImmutableSet<string> FAILED_STATUS =
            ImmutableHashSet.Create(
                [
                "Throttled",
                "Failed",
                "PartiallySucceeded",
                "Abandoned",
                "BadInput",
                "Canceled",
                "Skipped"
                ]);

        public AwaitExportedRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            DbClientFactory dbClientFactory,
            AzureBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(15))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var tasks = Parameterization.Activities.Values
                .GroupBy(a => a.GetSourceTableIdentity().ClusterUri)
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
            while (!AreActivitiesCompleted(activityNames))
            {
                var blockRecords = GetExportingBlocks(activityNames);

                await UpdateOperationsAsync(sourceClusterUri, blockRecords, ct);
                //  Sleep
                await SleepAsync(ct);
            }
        }

        private IImmutableList<BlockRecord> GetExportingBlocks(IEnumerable<string> activityNames)
        {
            var blockRecords = Database.Blocks.Query()
                .Where(pf => pf.In(b => b.BlockKey.ActivityName, activityNames))
                .Where(pf => pf.Equal(b => b.State, BlockState.Exporting))
                .Take(MAX_OPERATIONS)
                .ToImmutableArray();

            return blockRecords;
        }

        private async Task UpdateOperationsAsync(
            Uri clusterUri,
            IEnumerable<BlockRecord> blockRecords,
            CancellationToken ct)
        {
            var dbClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var operationIdMap = blockRecords
                .ToImmutableDictionary(b => b.ExportOperationId);
            var statuses = await dbClient.ShowOperationsAsync(
                KustoPriority.HighestPriority,
                operationIdMap.Keys,
                ct);

            DetectLostOperationIds(operationIdMap, statuses);
            DetectFailures(operationIdMap, statuses);
            await CompleteOperationsAsync(operationIdMap, statuses, ct);
        }

        #region Handle Operations
        private void DetectLostOperationIds(
            IImmutableDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<ExportOperationStatus> status)
        {
            var statusOperationIdBag = status
                .Select(s => s.OperationId)
                .ToHashSet();

            foreach (var id in operationIdMap.Keys)
            {
                if (!statusOperationIdBag.Contains(id))
                {
                    var block = operationIdMap[id];

                    TraceWarning(
                        $"Warning!  Operation ID lost:  '{id}' for " +
                        $"block {block.BlockKey.BlockId} (Iteration={block.BlockKey.IterationId}, " +
                        $"Activity='{block.BlockKey.ActivityName}') ; block marked for reprocessing");
                    block = block with
                    {
                        ExportOperationId = string.Empty,
                        State = BlockState.Planned
                    };
                    using (var tx = Database.Database.CreateTransaction())
                    {
                        Database.Blocks.Query(tx)
                            .Where(pf => pf.MatchKeys(
                                block,
                                b => b.BlockKey.ActivityName,
                                b => b.BlockKey.IterationId,
                                b => b.BlockKey.BlockId))
                            .Delete();
                        Database.Blocks.AppendRecord(block, tx);

                        tx.Complete();
                    }
                }
            }
        }

        private void DetectFailures(
            IImmutableDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<ExportOperationStatus> statuses)
        {
            var failedStatuses = statuses
                .Where(s => FAILED_STATUS.Contains(s.State));

            foreach (var status in failedStatuses)
            {
                var block = operationIdMap[status.OperationId];
                var message = status.ShouldRetry
                    ? "block marked for reprocessing"
                    : "block can't be re-exported";
                var warning = $"Warning!  Operation ID in state '{status.State}', " +
                    $"status '{status.Status}' " +
                    $"block {block.BlockKey.BlockId} (Iteration={block.BlockKey.IterationId}, " +
                    $"Activity='{block.BlockKey.ActivityName}') ; {message}";

                TraceWarning(warning);
                if (status.ShouldRetry)
                {
                    block = block with
                    {
                        ExportOperationId = string.Empty,
                        State = BlockState.Planned
                    };
                    using (var tx = Database.Database.CreateTransaction())
                    {
                        Database.Blocks.Query(tx)
                            .Where(pf => pf.MatchKeys(
                                block,
                                b => b.BlockKey.ActivityName,
                                b => b.BlockKey.IterationId,
                                b => b.BlockKey.BlockId))
                            .Delete();
                        Database.Blocks.AppendRecord(block, tx);

                        tx.Complete();
                    }
                }
                else
                {
                    throw new CopyException($"Permanent export error", false);
                }
            }
        }

        private async Task CompleteOperationsAsync(
            IImmutableDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<ExportOperationStatus> statuses,
            CancellationToken ct)
        {
            var tasks = statuses
                    .Where(s => s.State == "Completed")
                    .Select(s => ProcessOperationAsync(s, operationIdMap[s.OperationId], ct))
                    .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(tasks);
        }

        private async Task ProcessOperationAsync(
            ExportOperationStatus status,
            BlockRecord block,
            CancellationToken ct)
        {
            var activityParam = Parameterization.Activities[block.BlockKey.ActivityName];
            var sourceTable = activityParam.GetSourceTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                sourceTable.ClusterUri,
                sourceTable.DatabaseName);
            var details = await dbClient.ShowExportDetailsAsync(
                new KustoPriority(block.BlockKey),
                status.OperationId,
                ct);
            var urls = details
                .Select(d => new BlobUrlRecord(
                    UrlState.Exported,
                    block.BlockKey,
                    d.BlobUri,
                    d.RecordCount,
                    string.Empty))
                .ToImmutableArray();
            var newBlock = block with
            {
                State = BlockState.Exported,
                ExportOperationId = string.Empty,
                ExportedRowCount = details.Sum(d => d.RecordCount)
            };

            Trace.TraceInformation($"Exported block {block.BlockKey}:  {urls.Count()} urls");
            using (var tx = Database.Database.CreateTransaction())
            {
                Database.Blocks.Query(tx)
                    .Where(pf => pf.MatchKeys(
                        newBlock,
                        b => b.BlockKey.ActivityName,
                        b => b.BlockKey.IterationId,
                        b => b.BlockKey.BlockId))
                    .Delete();
                Database.BlobUrls.AppendRecords(urls, tx);
                Database.Blocks.AppendRecord(newBlock, tx);

                tx.Complete();
            }
            ValidatePlannedRowCount(newBlock, urls);
        }

        private void ValidatePlannedRowCount(
            BlockRecord block,
            IImmutableList<BlobUrlRecord> urls)
        {
            var exportedRowCount = urls.Sum(u => u.RowCount);

            if (block.PlannedRowCount != exportedRowCount)
            {
                TraceWarning($"Warning!  Block {block.BlockKey} " +
                    $"had planned row count of {block.PlannedRowCount} but " +
                    $"exported {exportedRowCount} rows");
            }
        }
        #endregion
    }
}