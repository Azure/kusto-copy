using Azure.Core;
using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;

namespace KustoCopyConsole.Runner
{
    internal class AwaitIngestRunner : ActivityRunnerBase
    {
        private const int MAX_BLOCK_COUNT = 25;

        public AwaitIngestRunner(
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
                 TimeSpan.FromSeconds(15))
        {
        }

        protected override async Task<bool> RunActivityAsync(string activityName, CancellationToken ct)
        {
            var destinationTable =
                Parameterization.Activities[activityName].Destination.GetTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                destinationTable.ClusterUri,
                destinationTable.DatabaseName);
            var iterationIds = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                .Where(pf => pf.LessThan(i => i.State, IterationState.Completed))
                .Select(i => i.IterationKey.IterationId)
                .ToImmutableArray();

            foreach (var iterationId in iterationIds)
            {
                var iterationKey = new IterationKey(activityName, iterationId);

                await UpdateIngestedAsync(iterationKey, dbClient, ct);
                await FailureDetectionAsync(iterationKey, destinationTable, ct);
            }
         
            return iterationIds.Any();
        }

        #region Update Ingested
        private async Task UpdateIngestedAsync(
            IterationKey iterationKey,
            DbCommandClient dbClient,
            CancellationToken ct)
        {
            var queuedBlockByBlockId = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, iterationKey.ActivityName))
                .Where(pf => pf.Equal(b => b.BlockKey.IterationId, iterationKey.IterationId))
                .Where(pf => pf.Equal(b => b.State, BlockState.Queued))
                .OrderBy(b => b.BlockKey.BlockId)
                .Take(MAX_BLOCK_COUNT)
                .ToImmutableDictionary(b => b.BlockKey.BlockId);

            if (queuedBlockByBlockId.Any())
            {
                var tempTable = Database.TempTables.Query()
                    .Where(pf => pf.Equal(t => t.IterationKey.ActivityName, iterationKey.ActivityName))
                    .Where(pf => pf.Equal(t => t.IterationKey.IterationId, iterationKey.IterationId))
                    .Take(1)
                    .FirstOrDefault();

                if (tempTable == null)
                {
                    throw new InvalidDataException(
                        $"TempTable for iteration " +
                        $"{queuedBlockByBlockId.Values.First().BlockKey.ToIterationKey()}" +
                        $" should exist by now");
                }

                var extents = await DetectIngestedBlocksAsync(
                    queuedBlockByBlockId.Values,
                    dbClient,
                    tempTable.TempTableName,
                    ct);

                using (var tx = Database.Database.CreateTransaction())
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
                    await tx.LogAndCompleteAsync();
                }
            }
        }

        private async Task<IEnumerable<ExtentRecord>> DetectIngestedBlocksAsync(
            IEnumerable<BlockRecord> blocks,
            DbCommandClient dbClient,
            string tempTableName,
            CancellationToken ct)
        {
            var blockKey = blocks.First().BlockKey;
            var allExtentRowCounts = await dbClient.GetExtentRowCountsAsync(
                new KustoPriority(blockKey.ToIterationKey()),
                blocks.Select(b => b.BlockTag),
                tempTableName,
                ct);
            var extentRowCountByTags = allExtentRowCounts
                .GroupBy(e => e.Tags)
                .ToImmutableDictionary(g => g.Key);
            var blockByTags = blocks
                .ToImmutableDictionary(b => b.BlockTag);
            var targetRowCountByBlockId = Database.BlobUrls.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, blockKey.ActivityName))
                .Where(pf => pf.Equal(b => b.BlockKey.IterationId, blockKey.IterationId))
                .Where(pf => pf.Equal(b => b.BlockKey.BlockId, blockKey.BlockId))
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
                            .Select(e => new ExtentRecord(blockKey, e.ExtentId, e.RecordCount));

                        extents.AddRange(blockExtents);
                    }
                }
            }

            return extents;
        }
        #endregion

        #region Failure detection
        private async Task FailureDetectionAsync(
            IterationKey iterationKey,
            TableIdentity destinationTable,
            CancellationToken ct)
        {
            var oldestQueuedBlock = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, iterationKey.ActivityName))
                .Where(pf => pf.Equal(b => b.BlockKey.IterationId, iterationKey.IterationId))
                .Where(pf => pf.Equal(b => b.State, BlockState.Queued))
                .OrderBy(b => b.BlockKey.BlockId)
                .Take(1)
                .FirstOrDefault();

            if (oldestQueuedBlock != null)
            {
                var tempTable = Database.TempTables.Query()
                    .Where(pf => pf.Equal(t => t.IterationKey.ActivityName, iterationKey.ActivityName))
                    .Where(pf => pf.Equal(t => t.IterationKey.IterationId, iterationKey.IterationId))
                    .Take(1)
                    .FirstOrDefault();

                if (tempTable == null)
                {
                    throw new InvalidDataException(
                        $"TempTable for iteration {iterationKey} should exist by now");
                }

                var ingestClient = DbClientFactory.GetIngestClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName,
                    tempTable.TempTableName);
                var blobUrls = Database.BlobUrls.Query()
                    .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, iterationKey.ActivityName))
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationId, iterationKey.IterationId))
                    .Where(pf => pf.Equal(b => b.BlockKey.BlockId, oldestQueuedBlock.BlockKey.BlockId))
                    .ToImmutableArray();

                foreach (var blobUrl in blobUrls)
                {
                    var failure = await ingestClient.FetchIngestionFailureAsync(
                        blobUrl.SerializedQueuedResult);

                    if (failure != null)
                    {
                        TraceWarning(
                            $"Warning!  Ingestion failed with status '{failure.Status}'" +
                            $"and detail '{failure.Details}' for blob {blobUrl.Url} in block " +
                            $"{oldestQueuedBlock.BlockKey} ; block will be re-exported");
                        ReturnToPlanned(oldestQueuedBlock);

                        return;
                    }
                }
            }
        }

        private void ReturnToPlanned(BlockRecord block)
        {
            block = block with
            {
                State = BlockState.Planned,
                ExportOperationId = string.Empty,
                BlockTag = string.Empty
            };

            using (var tx = Database.Database.CreateTransaction())
            {
                Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, block.BlockKey.ActivityName))
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationId, block.BlockKey.IterationId))
                    .Where(pf => pf.Equal(b => b.BlockKey.BlockId, block.BlockKey.BlockId))
                    .Delete();
                Database.BlobUrls.Query(tx)
                    .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, block.BlockKey.ActivityName))
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationId, block.BlockKey.IterationId))
                    .Where(pf => pf.Equal(b => b.BlockKey.BlockId, block.BlockKey.BlockId))
                    .Delete();
                Database.Blocks.AppendRecord(block, tx);

                tx.Complete();
            }
        }
        #endregion
    }
}