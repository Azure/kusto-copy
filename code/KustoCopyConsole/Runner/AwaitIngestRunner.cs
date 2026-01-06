using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class AwaitIngestRunner : ActivityRunnerBase
    {
        private const int MAX_BLOCK_COUNT = 25;

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
            var iterationIds = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                .Where(pf => pf.In(i => i.State, [IterationState.Planning, IterationState.Planned]))
                .Select(i => i.IterationKey.IterationId)
                .ToImmutableArray();

            foreach (var iterationId in iterationIds)
            {
                var iterationKey = new IterationKey(activityName, iterationId);

                await UpdateIngestedAsync(iterationKey, dbClient, ct);
                await FailureDetectionAsync(iterationKey, destinationTable, ct);
            }
            //  This is done separately to ensure a failure between db and logs wouldn't result
            //  in blocks being moved
            await UpdateReadyToMoveAsync(activityName, ct);

            return iterationIds.Any();
        }

        #region Update Ingested
        private async Task UpdateIngestedAsync(
            IterationKey iterationKey,
            DbCommandClient dbClient,
            CancellationToken ct)
        {
            var queuedBlockByBlockId = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                .Where(pf => pf.Equal(b => b.State, BlockState.Queued))
                .OrderBy(b => b.BlockKey.BlockId)
                .Take(MAX_BLOCK_COUNT)
                .ToImmutableDictionary(b => b.BlockKey.BlockId);

            if (queuedBlockByBlockId.Any())
            {
                var tempTable = GetTempTable(iterationKey);
                var extents = await DetectIngestedBlocksAsync(
                    queuedBlockByBlockId.Values,
                    iterationKey,
                    dbClient,
                    tempTable.TempTableName,
                    ct);

                using (var tx = Database.CreateTransaction())
                {
                    var ingestedBlockIds = extents
                        .Select(e => e.BlockKey.BlockId)
                        .Distinct();
                    var ingestedBlocks = ingestedBlockIds
                        .Select(id => queuedBlockByBlockId[id])
                        .Select(block => block with
                        {
                            State = BlockState.Ingested
                        });

                    Database.Blocks.Query(tx)
                        .Where(pf => pf.In(b => b.BlockKey.BlockId, ingestedBlockIds))
                        .Delete();
                    Database.Blocks.AppendRecords(ingestedBlocks, tx);
                    Database.Extents.AppendRecords(extents, tx);

                    //  We do wait for the ingested status to persist before moving
                    //  This is to avoid moving extents before the confirmation of
                    //  ingestion is persisted:  this would result in the block
                    //  staying in "queued" if the process would restart
                    await tx.CompleteAsync(ct);
                }
            }
        }

        private async Task<IEnumerable<ExtentRecord>> DetectIngestedBlocksAsync(
            IEnumerable<BlockRecord> blocks,
            IterationKey iterationKey,
            DbCommandClient dbClient,
            string tempTableName,
            CancellationToken ct)
        {
            var allExtentRowCounts = await dbClient.GetExtentRowCountsAsync(
                new KustoPriority(iterationKey),
                blocks.Select(b => b.BlockTag),
                tempTableName,
                ct);
            var extentRowCountByTags = allExtentRowCounts
                .GroupBy(e => e.Tags)
                .ToImmutableDictionary(g => g.Key);
            var blockByTags = blocks
                .ToImmutableDictionary(b => b.BlockTag);
            var targetRowCountByBlockId = Database.BlobUrls.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                .Where(pf => pf.In(b => b.BlockKey.BlockId, blocks.Select(b => b.BlockKey.BlockId)))
                .AsEnumerable()
                .GroupBy(u => u.BlockKey.BlockId)
                .ToImmutableDictionary(g => g.Key, g => g.Sum(u => u.RowCount));
            var extents = new List<ExtentRecord>();

            Trace.TraceInformation($"AwaitIngest:  {allExtentRowCounts.Count} " +
                $"extents found with {extentRowCountByTags.Count} tags");
            foreach (var tag in extentRowCountByTags.Keys)
            {
                if (extentRowCountByTags.TryGetValue(tag, out var extentRowCounts)
                    && blockByTags.TryGetValue(tag, out var block)
                    && targetRowCountByBlockId.TryGetValue(block.BlockKey.BlockId, out var targetRowCount))
                {
                    var blockExtentRowCount = extentRowCounts
                        .Sum(e => e.RecordCount);

                    if (blockExtentRowCount > targetRowCount)
                    {
                        throw new CopyException(
                            $"Target row count is {targetRowCount} while " +
                            $"we observe {blockExtentRowCount}",
                            false);
                    }
                    if (blockExtentRowCount == targetRowCount)
                    {
                        var blockExtents = extentRowCounts
                            .Select(e => new ExtentRecord(block.BlockKey, e.ExtentId, e.RecordCount));

                        extents.AddRange(blockExtents);
                    }
                }
            }

            return extents;
        }
        #endregion

        #region Update Ready to move
        private async Task UpdateReadyToMoveAsync(string activityName, CancellationToken ct)
        {
            var ingestedBlocks = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey.ActivityName, activityName))
                .Where(pf => pf.Equal(b => b.State, BlockState.Ingested))
                .ToImmutableArray();

            using (var tx = Database.CreateTransaction())
            {
                var readyToMoveBlocks = ingestedBlocks
                    .Select(block => block with
                    {
                        State = BlockState.ReadyToMove
                    });

                Database.Blocks.Query(tx)
                    .Where(pf => pf.In(
                        b => b.BlockKey.BlockId,
                        ingestedBlocks.Select(b => b.BlockKey.BlockId)))
                    .Delete();
                Database.Blocks.AppendRecords(readyToMoveBlocks, tx);

                tx.Complete();
            }
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