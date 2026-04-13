using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class AwaitExportedRunner : AwaitCommandRunner
    {
        public AwaitExportedRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(10))
        {
        }

        protected override BlockState InitialState => BlockState.Exporting;

        protected override BlockState ResetState => BlockState.Planned;

        protected override Uri GetClusterUri(ActivityParameterization activity)
            => activity.GetSourceTableIdentity().ClusterUri;

        protected override BlockRecord ResetBlock(BlockRecord block)
        {
            return block with { ExportOperationId = string.Empty };
        }

        protected override string GetOperationId(BlockRecord block)
        {
            return block.ExportOperationId;
        }

        protected override async Task ProcessOperationAsync(
            OperationStatus status,
            BlockRecord block,
            DbCommandClient dbClient,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var details = await dbClient.ShowExportDetailsAsync(
                new KustoPriority(block.BlockKey),
                status.OperationId,
                ct);
            var urls = details
                .Select(d => new BlobUrlRecord(
                    block.BlockKey,
                    d.BlobUri,
                    d.RecordCount))
                .ToArray();

            if (urls.Length > 0)
            {
                var newBlock = block with
                {
                    State = BlockState.Exported,
                    ExportOperationId = string.Empty,
                    ExportedRowCount = details.Sum(d => d.RecordCount)
                };

                using (var tx = Database.CreateTransaction())
                {
                    Database.Blocks.UpdateRecord(block, newBlock, tx);
                    Database.BlobUrls.AppendRecords(urls, tx);

                    tx.Complete();
                }
            }
            else
            {
                Database.Blocks.UpdateRecord(
                    block,
                    block with
                    {
                        ExportOperationId = string.Empty,
                        State = BlockState.ExtentMoved,
                        ExportedRowCount = 0
                    });
            }
        }
    }
}