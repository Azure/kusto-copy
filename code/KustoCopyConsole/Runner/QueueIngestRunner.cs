using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class QueueIngestRunner : ActivityRunnerBase
    {
        const int PARALLEL_BLOCKS = 20;
        const int BATCH_BLOCKS = 200;

        public QueueIngestRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(5))
        {
        }

        protected override async Task<bool> RunActivityAsync(
            string activityName,
            CancellationToken ct)
        {
            var destinationTable = Parameterization.Activities[activityName]
                .GetDestinationTableIdentity();
            var blockQueue = new Queue<BlockRecord>(Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey.ActivityName, activityName))
                .Where(pf => pf.Equal(b => b.State, BlockState.Exported))
                .OrderBy(b => b.BlockKey.IterationKey.IterationId)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(BATCH_BLOCKS));
            var tempTableMap = blockQueue
                .Select(b => b.BlockKey.IterationKey)
                .Distinct()
                .Select(key => new
                {
                    Key = key,
                    TempTable = TryGetTempTable(key)?.TempTableName
                })
                //  It's possible, although unlikely, the temp table hasn't been created yet
                .Where(o => o.TempTable != null)
                .ToImmutableDictionary(o => o.Key, o => o.TempTable!);

            if (blockQueue.Any() && tempTableMap.Any())
            {
                var ingestClient = DbClientFactory.GetIngestClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName);
                var taskQueue = new Queue<Task>(PARALLEL_BLOCKS);

                while (blockQueue.Count > 0 || taskQueue.Count > 0)
                {
                    if (taskQueue.Count >= PARALLEL_BLOCKS || blockQueue.Count == 0)
                    {   //  Wait for one
                        var task = taskQueue.Dequeue();

                        await task;
                    }
                    else
                    {
                        var block = blockQueue.Dequeue();

                        if (tempTableMap.ContainsKey(block.BlockKey.IterationKey))
                        {
                            var task = QueueIngestBlockAsync(
                                block,
                                ingestClient,
                                tempTableMap[block.BlockKey.IterationKey],
                                ct);

                            taskQueue.Enqueue(task);
                        }
                    }
                }

                return true;
            }
            else
            {   //  No blocks or temp table was available, we'll process this block later
                return false;
            }
        }

        private async Task QueueIngestBlockAsync(
            BlockRecord block,
            IngestClient ingestClient,
            string tempTableName,
            CancellationToken ct)
        {
            var urlRecords = Database.BlobUrls.Query()
                .Where(pf => pf.Equal(u => u.BlockKey, block.BlockKey))
                .ToImmutableArray();

            Trace.TraceInformation($"Block {block.BlockKey}:  ingest {urlRecords.Length} urls");

            var blockTag = $"drop-by:kusto-copy|{Guid.NewGuid()}";
            //  Get Uri with SAS tokens
            var authorizedUriTasks = urlRecords
                .Select(u => StagingBlobUriProvider.AuthorizeUriAsync(u.Url, ct))
                .ToImmutableArray();

            await Task.WhenAll(authorizedUriTasks);

            var authorizedUris = authorizedUriTasks
                .Select(t => t.Result)
                .ToImmutableList();
            //  Queue all blobs
            var operationTexts = await ingestClient.QueueBlobsAsync(
                new KustoPriority(block.BlockKey),
                tempTableName,
                authorizedUris,
                blockTag,
                block.CreationTime,
                ct);
            var ingestionBatches = operationTexts
                .Select(op => new IngestionBatchRecord(block.BlockKey, op));

            using (var tc = Database.CreateTransaction())
            {
                Database.Blocks.UpdateRecord(
                    block,
                    block with
                    {
                        State = BlockState.Queued,
                        BlockTag = blockTag
                    },
                    tc);
                Database.IngestionBatches.AppendRecords(ingestionBatches);

                tc.Complete();
            }

            Trace.TraceInformation($"Block {block.BlockKey}:  {urlRecords.Length} urls queued");
        }
    }
}