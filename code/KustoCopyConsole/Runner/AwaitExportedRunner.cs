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
            IEnumerable<BlockRecord> blocks,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var iterationKey = blocks.First().BlockKey.IterationKey;
            var tableId = activityParam.GetDestinationTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                tableId.ClusterUri,
                tableId.DatabaseName);
            var taskBundles = blocks
                .Select(b => new
                {
                    Block = b,
                    DetailTask = dbClient.ShowExportDetailsAsync(
                        new KustoPriority(b.BlockKey),
                        b.ExportOperationId,
                        ct)
                })
                .ToArray();

            await Task.WhenAll(taskBundles.Select(b => b.DetailTask));

            var newBlocks = taskBundles
                .Select(tb => tb.DetailTask.Result.Count > 0
                ? tb.Block with
                {
                    State = BlockState.Exported,
                    ExportOperationId = string.Empty,
                    ExportedRowCount = tb.DetailTask.Result.Sum(d => d.RecordCount)
                }
                : tb.Block with
                {
                    ExportOperationId = string.Empty,
                    State = BlockState.ExtentMoved,
                    ExportedRowCount = 0
                });
            var newUrls = taskBundles
                .SelectMany(tb => tb.DetailTask.Result.Select(d => new BlobUrlRecord(
                    tb.Block.BlockKey,
                    d.BlobUri,
                    d.RecordCount)));

            using (var tx = Database.CreateTransaction())
            {
                Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                    .Where(pf => pf.In(
                        b => b.BlockKey.BlockId,
                        blocks.Select(b => b.BlockKey.BlockId)))
                    .Delete();
                Database.Blocks.AppendRecords(newBlocks, tx);
                Database.BlobUrls.AppendRecords(newUrls, tx);

                tx.Complete();
            }
        }
    }
}