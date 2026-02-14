using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class AwaitIngestRunner : ActivityRunnerBase
    {
        private const int MAX_BLOCK_INGESTED_COUNT = 500;
        private const int MAX_BLOCK_EXTENT_COUNT = 50;

        public AwaitIngestRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(15))
        {
        }

        protected override async Task<bool> RunActivityAsync(string activityName, CancellationToken ct)
        {
            var destinationTable =
                Parameterization.Activities[activityName].GetDestinationTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                destinationTable.ClusterUri,
                destinationTable.DatabaseName);
            var iterationKeys = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                .Where(pf => pf.In(i => i.State, [IterationState.Planning, IterationState.Planned]))
                .Select(i => i.IterationKey)
                .ToImmutableArray();

            foreach (var iterationKey in iterationKeys)
            {
                while (await UpdateIngestedAsync(iterationKey, dbClient, ct))
                {
                }
                await FailureDetectionAsync(iterationKey, destinationTable, ct);
            }

            return iterationKeys.Any();
        }

        #region Update Ingested
        private async Task<bool> UpdateIngestedAsync(
            IterationKey iterationKey,
            DbCommandClient dbClient,
            CancellationToken ct)
        {
            var tempTable = GetTempTable(iterationKey);
            var ingestedBlocks = await DetectIngestedBlocksAsync(
                iterationKey,
                dbClient,
                tempTable.TempTableName,
                ct);

            if (ingestedBlocks.Any())
            {
                var extentRowCounts = await dbClient.GetExtentRowCountsAsync(
                    new KustoPriority(iterationKey),
                    ingestedBlocks.Select(b => b.BlockTag),
                    tempTable.TempTableName,
                    ct);
                var blocksByTag = ingestedBlocks
                    .ToImmutableDictionary(b => b.BlockTag);
                var extents = extentRowCounts
                    .Select(erc => new ExtentRecord(
                        blocksByTag[erc.Tags].BlockKey,
                        erc.ExtentId,
                        erc.RecordCount));

                using (var tx = Database.CreateTransaction())
                {
                    var ingestedBlockIds = ingestedBlocks
                        .Select(b => b.BlockKey.BlockId);

                    Database.Blocks.Query(tx)
                        .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                        .Where(pf => pf.In(b => b.BlockKey.BlockId, ingestedBlockIds))
                        .Delete();
                    Database.IngestionBatches.Query(tx)
                        .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                        .Where(pf => pf.In(b => b.BlockKey.BlockId, ingestedBlockIds))
                        .Delete();
                    Database.BlobUrls.Query(tx)
                        .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                        .Where(pf => pf.In(b => b.BlockKey.BlockId, ingestedBlockIds))
                        .Delete();

                    Database.Blocks.AppendRecords(
                        ingestedBlocks
                        .Select(b => b with { State = BlockState.Ingested }),
                        tx);
                    Database.Extents.AppendRecords(extents, tx);

                    tx.Complete();
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private async Task<IEnumerable<BlockRecord>> DetectIngestedBlocksAsync(
            IterationKey iterationKey,
            DbCommandClient dbClient,
            string tempTableName,
            CancellationToken ct)
        {
            var queuedBlocks = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                .Where(pf => pf.Equal(b => b.State, BlockState.Queued))
                .OrderBy(b => b.BlockKey.BlockId)
                .Take(MAX_BLOCK_INGESTED_COUNT)
                .ToImmutableArray();

            if (queuedBlocks.Length > 0)
            {
                var tagRowCounts = await dbClient.GetIngestedTagRowCountsAsync(
                    new KustoPriority(iterationKey),
                    queuedBlocks.Select(b => b.BlockTag),
                    queuedBlocks.Select(b => b.ExportedRowCount),
                    tempTableName,
                    ct);

                if (tagRowCounts.Any())
                {
                    var queuedBlocksByTags = queuedBlocks.ToImmutableDictionary(b => b.BlockTag);
                    var overIngestedBlock = tagRowCounts
                        .Select(trc => new
                        {
                            RowCount = trc.RecordCount,
                            Block = queuedBlocksByTags[trc.Tags]
                        })
                        .Where(o => o.Block.ExportedRowCount < o.RowCount)
                        .FirstOrDefault();

                    if (overIngestedBlock != null)
                    {
                        throw new CopyException(
                            $"Exported row count is {overIngestedBlock.Block.ExportedRowCount} " +
                            $"while we ingested {overIngestedBlock.RowCount} rows for block " +
                            $"{overIngestedBlock.Block.BlockKey}",
                            false);
                    }

                    return tagRowCounts
                        .Select(trc => queuedBlocksByTags[trc.Tags])
                        .Take(MAX_BLOCK_EXTENT_COUNT)
                        .ToImmutableArray();
                }
            }

            return Array.Empty<BlockRecord>();
        }
        #endregion

        #region Failure detection
        private async Task FailureDetectionAsync(
            IterationKey iterationKey,
            TableIdentity destinationTable,
            CancellationToken ct)
        {
            var oldestQueuedBlock = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                .Where(pf => pf.Equal(b => b.State, BlockState.Queued))
                .OrderBy(b => b.BlockKey.BlockId)
                .Take(1)
                .FirstOrDefault();

            if (oldestQueuedBlock != null)
            {
                var ingestClient = DbClientFactory.GetIngestClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName);
                var ingestionBatches = Database.IngestionBatches.Query()
                    .Where(pf => pf.Equal(b => b.BlockKey, oldestQueuedBlock.BlockKey))
                    .ToImmutableArray();

                foreach (var batch in ingestionBatches)
                {
                    var isFailure = await ingestClient.IsIngestionFailureAsync(
                        new KustoPriority(oldestQueuedBlock.BlockKey),
                        batch.OperationText,
                        ct);

                    if (isFailure)
                    {
                        TraceWarning(
                            $"Warning!  Ingestion failed for block {oldestQueuedBlock.BlockKey} " +
                            $"; block will be re-exported");
                        ReturnToPlanned(oldestQueuedBlock);

                        return;
                    }
                }
            }
        }

        private void ReturnToPlanned(BlockRecord block)
        {
            using (var tx = Database.CreateTransaction())
            {
                Database.Blocks.UpdateRecord(
                    block,
                    block with
                    {
                        State = BlockState.Planned,
                        ExportOperationId = string.Empty,
                        BlockTag = string.Empty
                    },
                    tx);
                Database.BlobUrls.Query(tx)
                    .Where(pf => pf.Equal(u => u.BlockKey, block.BlockKey))
                    .Delete();
                Database.IngestionBatches.Query(tx)
                    .Where(pf => pf.Equal(i => i.BlockKey, block.BlockKey))
                    .Delete();

                tx.Complete();
            }
        }
        #endregion
    }
}