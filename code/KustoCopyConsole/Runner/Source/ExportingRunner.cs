using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner.Source
{
    internal class ExportingRunner : StartAwaitCommandRunnerBase
    {
        public ExportingRunner(RunnerParameters parameters)
           : base(parameters, true, TimeSpan.FromSeconds(10))
        {
        }

        public override async Task RunAsync(CancellationToken ct)
        {
            if (ShouldExportRun)
            {
                await base.RunAsync(ct);
            }
        }

        protected override int? MaxCapacity => Parameterization.ExportCount;

        protected override BlockState InitialState => BlockState.Planned;

        protected override BlockState TransitionState => BlockState.Exporting;

        protected override BlockRecord ResetBlock(BlockRecord block)
        {
            return block with
            {
                State = BlockState.Planned,
                ExportOperationId = string.Empty
            };
        }

        protected override async Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var capacity = await dbCommandClient.ShowExportCapacityAsync(
                KustoPriority.HighestPriority,
                ct);

            return capacity;
        }

        protected override async Task<BlockRecord> StartBlockAsync(
            BlockRecord blockRecord,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var sourceTable = activityParam.GetSourceTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                sourceTable.ClusterUri,
                sourceTable.DatabaseName);
            var writableUris = await StagingBlobUriProvider.GetWritableFolderUrisAsync(
                blockRecord.BlockKey,
                ct);
            var iterationRecord = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, blockRecord.BlockKey.IterationKey))
                .First();
            var operationId = await dbClient.ExportBlockAsync(
                new KustoPriority(blockRecord.BlockKey),
                writableUris,
                sourceTable.TableName,
                activityParam.KqlQuery,
                activityParam.KqlAggregationQuery,
                iterationRecord.CursorStart,
                iterationRecord.CursorEnd,
                blockRecord.IngestionTimeStart,
                blockRecord.IngestionTimeEnd,
                ct);
            var newBlockRecord = blockRecord with
            {
                ExportOperationId = operationId
            };

            return newBlockRecord;
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
            var tableId = activityParam.GetSourceTableIdentity();
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
                })
                .ToArray();
            var newUrls = taskBundles
                .SelectMany(tb => tb.DetailTask.Result.Select(d => new BlobUrlRecord(
                    tb.Block.BlockKey,
                    d.BlobUri,
                    d.RecordCount)));

            if (newBlocks.Length > 0)
            {
                using (var tx = Database.CreateTransaction())
                {
                    var hasTempTable = Database.TempTables.Query(tx)
                        .Where(pf => pf.Equal(t => t.IterationKey, iterationKey))
                        .Any();

                    Database.Blocks.Query(tx)
                        .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                        .Where(pf => pf.In(
                            b => b.BlockKey.BlockId,
                            blocks.Select(b => b.BlockKey.BlockId)))
                        .Delete();
                    Database.Blocks.AppendRecords(newBlocks, tx);
                    Database.BlobUrls.AppendRecords(newUrls, tx);
                    //  Create a temp table requirement only once per iteration.
                    //  Temp table is required JIT:  no block, no temp table.
                    if (!hasTempTable)
                    {
                        Database.TempTables.AppendRecord(
                            new TempTableRecord(
                                TempTableState.Required,
                                iterationKey,
                                string.Empty),
                            tx);
                    }

                    tx.Complete();
                }
            }
        }
    }
}