using Azure.Core;
using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Db;
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
    internal class AwaitIngestRunner : RunnerBase
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

        public async Task RunAsync(CancellationToken ct)
        {
            var tasks = Parameterization.Activities.Keys
                .Select(a => RunActivityAsync(a, ct))
                .ToImmutableList();

            await Task.WhenAll(tasks);
        }

        private async Task RunActivityAsync(string activityName, CancellationToken ct)
        {
            var destinationTable =
                Parameterization.Activities[activityName].Destination.GetTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                destinationTable.ClusterUri,
                destinationTable.DatabaseName);

            while (!IsActivityCompleted(activityName))
            {
                var iterationIds = Database.Iterations.Query()
                    .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                    .Where(pf => pf.LessThan(i => i.State, IterationState.Completed))
                    .Select(i => i.IterationKey.IterationId)
                    .ToImmutableArray();

                foreach (var iterationId in iterationIds)
                {
                    await UpdateIngestedAsync(new IterationKey(activityName, iterationId), dbClient, ct);
                    await FailureDetectionAsync(ct);
                }
                if (iterationIds.Any())
                {
                    await SleepAsync(ct);
                }
            }
        }

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
                    var ingestedBlocks = extents
                        .Select(e => e.BlockKey.BlockId)
                        .Distinct()
                        .Select(id => queuedBlockByBlockId[id])
                        .Select(block => block with
                        {
                            State = BlockState.Ingested
                        });

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

        private async Task FailureDetectionAsync(CancellationToken ct)
        {
            var activeIterations = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                .SelectMany(a => a.IterationMap.Values)
                .Where(i => i.RowItem.State != IterationState.Completed);
            var iterationTasks = activeIterations
                .Select(i => IterationFailureDetectionAsync(i, ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(iterationTasks);
        }

        private async Task IterationFailureDetectionAsync(
            IterationCache iterationCache,
            CancellationToken ct)
        {
            var queuedBlocks = iterationCache
                .BlockMap
                .Values
                .Where(b => b.RowItem.State == BlockState.Queued);

            if (queuedBlocks.Any())
            {
                var activityItem = RowItemGateway.InMemoryCache
                    .ActivityMap[iterationCache.RowItem.ActivityName]
                    .RowItem;
                var ingestClient = DbClientFactory.GetIngestClient(
                    activityItem.DestinationTable.ClusterUri,
                    activityItem.DestinationTable.DatabaseName,
                    iterationCache.TempTable!.TempTableName);
                var oldestBlock = queuedBlocks
                    .ArgMin(b => b.RowItem.Updated);

                foreach (var urlItem in oldestBlock.UrlMap.Values.Select(u => u.RowItem))
                {
                    var failure = await ingestClient.FetchIngestionFailureAsync(
                        urlItem.SerializedQueuedResult);

                    if (failure != null)
                    {
                        TraceWarning(
                            $"Warning!  Ingestion failed with status '{failure.Status}'" +
                            $"and detail '{failure.Details}' for blob {urlItem.Url} in block " +
                            $"{oldestBlock.RowItem.BlockId}, iteration " +
                            $"{oldestBlock.RowItem.IterationId}, activity " +
                            $"{oldestBlock.RowItem.ActivityName} ; block will be re-exported");
                        ReturnToPlanned(oldestBlock);

                        return;
                    }
                }
            }
        }

        private void ReturnToPlanned(BlockCache oldestBlock)
        {
            var newBlock = oldestBlock.RowItem.ChangeState(BlockState.Planned);

            newBlock.ExportOperationId = string.Empty;
            newBlock.BlockTag = string.Empty;
            RowItemGateway.Append(newBlock);
        }
    }
}