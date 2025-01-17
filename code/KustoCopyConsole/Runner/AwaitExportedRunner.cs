using Azure.Storage.Blobs.Models;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class AwaitExportedRunner : RunnerBase
    {
        #region Inner Types
        private record ClusterBlocks(Uri ClusterUri, IEnumerable<BlockRowItem> BlockItems);
        #endregion

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
            var exportingBlocks = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                .SelectMany(a => a.IterationMap.Values)
                .Where(i => i.RowItem.State != IterationState.Completed)
                .SelectMany(i => i.BlockMap.Values)
                .Select(b => b.RowItem)
                .Where(b => b.State == BlockState.Exporting);
            var clusterBlocks = exportingBlocks
                .Select(b => new
                {
                    RowItemGateway.InMemoryCache
                    .ActivityMap[b.ActivityName]
                    .RowItem
                    .SourceTable
                    .ClusterUri,
                    BlockItem = b
                })
                .GroupBy(o => o.ClusterUri, o => o.BlockItem)
                .Select(g => new ClusterBlocks(g.Key, g.ToImmutableArray()));
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
            var status = await dbClient.ShowOperationsAsync(
                KustoPriority.HighestPriority,
                operationIdMap.Keys,
                ct);

            throw new NotImplementedException();
        }

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