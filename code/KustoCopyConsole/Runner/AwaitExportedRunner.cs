using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class AwaitExportedRunner : RunnerBase
    {
        #region Inner Types
        private record ClusterBlocks(Uri ClusterUri, IEnumerable<BlockRowItem> BlockItems);
        #endregion

        private const int MAX_OPERATIONS = 50;
        private static readonly IImmutableSet<string> FAILED_STATUS =
            ImmutableHashSet.Create(
                [
                "Failed",
                "PartiallySucceeded",
                "Abandoned",
                "BadInput",
                "Canceled",
                "Skipped"
                ]);

        public AwaitExportedRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory, TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            //  Clean half-exported URLs
            CleanCompletingExports();
            while (!AllActivitiesCompleted())
            {
                var clusterBlocks = GetClusterBlocks();
                var tasks = clusterBlocks
                    .Select(o => UpdateOperationsAsync(o.ClusterUri, o.BlockItems, ct))
                    .ToImmutableArray();

                await Task.WhenAll(tasks);
                //  Sleep
                await SleepAsync(ct);
            }
        }

        private IEnumerable<ClusterBlocks> GetClusterBlocks()
        {
            var hierarchy = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.State != ActivityState.Completed,
                i => i.RowItem.State != IterationState.Completed);
            var exportingBlocks = hierarchy
                .Where(h => h.BlockItem.State == BlockState.Exporting);
            var clusterBlocks = exportingBlocks
                .GroupBy(h => h.Activity.SourceTable.ClusterUri)
                .Select(g => new ClusterBlocks(
                    g.Key,
                    g.Select(h => h.BlockItem).OrderBy(b => b.Updated).Take(MAX_OPERATIONS)));

            return clusterBlocks;
        }

        private async Task UpdateOperationsAsync(
            Uri clusterUri,
            IEnumerable<BlockRowItem> blockItems,
            CancellationToken ct)
        {
            var dbClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var operationIdMap = blockItems
                .ToImmutableDictionary(b => b.OperationId);
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
            IImmutableDictionary<string, BlockRowItem> operationIdMap,
            IImmutableList<ExportOperationStatus> status)
        {
            var statusOperationIdBag = status.Select(s => s.OperationId).ToHashSet();

            foreach (var id in operationIdMap.Keys)
            {
                if (!statusOperationIdBag.Contains(id))
                {
                    var block = operationIdMap[id];

                    Trace.TraceWarning($"Warning!  Operation ID lost:  '{id}' for " +
                        $"block {block.BlockId} (Iteration={block.IterationId}, " +
                        $"Activity='{block.ActivityName}') ; block marked for reprocessing");
                    block.OperationId = string.Empty;
                    block.ChangeState(BlockState.Planned);
                    RowItemGateway.Append(block);
                }
            }
        }

        private void DetectFailures(
            IImmutableDictionary<string, BlockRowItem> operationIdMap,
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
                    $"block {block.BlockId} (Iteration={block.IterationId}, " +
                    $"Activity='{block.ActivityName}') ; {message}";

                Trace.TraceWarning(warning);
                if (status.ShouldRetry)
                {
                    block.OperationId = string.Empty;
                    block.ChangeState(BlockState.Planned);
                    RowItemGateway.Append(block);
                }
                else
                {
                    throw new CopyException(warning, false);
                }
            }
        }
        private async Task CompleteOperationsAsync(
            IImmutableDictionary<string, BlockRowItem> operationIdMap,
            IImmutableList<ExportOperationStatus> statuses,
            CancellationToken ct)
        {
            async Task ProcessOperationAsync(
                ExportOperationStatus status,
                BlockRowItem block,
                CancellationToken ct)
            {
                var activity =
                    RowItemGateway.InMemoryCache.ActivityMap[block.ActivityName].RowItem;
                var dbClient = DbClientFactory.GetDbCommandClient(
                    activity.SourceTable.ClusterUri,
                    activity.SourceTable.DatabaseName);
                var details = await dbClient.ShowExportDetailsAsync(
                    new KustoPriority(block.GetIterationKey()),
                    status.OperationId,
                    ct);
                var urls = details
                    .Select(d => new UrlRowItem
                    {
                        State = UrlState.Exported,
                        ActivityName = block.ActivityName,
                        IterationId = block.IterationId,
                        BlockId = block.BlockId,
                        Url = d.BlobUri.ToString(),
                        RowCount = d.RecordCount
                    });

                RowItemGateway.Append(block.ChangeState(BlockState.CompletingExport));
                foreach (var url in urls)
                {
                    RowItemGateway.Append(url);
                }
                RowItemGateway.Append(block.ChangeState(BlockState.Exported));
            }

            var tasks = statuses
                .Where(s => s.State == "Completed")
                .Select(s => ProcessOperationAsync(s, operationIdMap[s.OperationId], ct))
                .ToImmutableArray();

            await Task.WhenAll(tasks);
        }
        #endregion

        private void CleanCompletingExports()
        {
            var completingBlocks = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                .SelectMany(a => a.IterationMap.Values)
                .Where(i => i.RowItem.State != IterationState.Completed)
                .SelectMany(i => i.BlockMap.Values)
                .Where(b => b.RowItem.State == BlockState.CompletingExport);

            foreach (var block in completingBlocks)
            {
                foreach (var url in block.UrlMap.Values)
                {
                    RowItemGateway.Append(url.RowItem.ChangeState(UrlState.Deleted));
                }
                RowItemGateway.Append(block.RowItem.ChangeState(BlockState.Exporting));
            }
        }
    }
}