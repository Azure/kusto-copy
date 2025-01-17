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
                //  Sleep
                await SleepAsync(ct);
            }
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