using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class QueueIngestRunner : ActivityRunnerBase
    {
        private const int BATCH_BLOCKS = 20;

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
            var blocks = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey.ActivityName, activityName))
                .Where(pf => pf.Equal(b => b.State, BlockState.Exported))
                .OrderBy(b => b.BlockKey.IterationKey.IterationId)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(BATCH_BLOCKS)
                .ToImmutableArray();

            if (blocks.Length > 0)
            {
                var iterationKey = blocks[0].BlockKey.IterationKey;
                var tempTable = TryGetTempTable(iterationKey);

                if (tempTable != null)
                {
                    //  Filter blocks with given iteration key
                    blocks = blocks
                        .Where(b => b.BlockKey.IterationKey == iterationKey)
                        .ToImmutableArray();

                    var ingestClient = DbClientFactory.GetIngestClient(
                        destinationTable.ClusterUri,
                        destinationTable.DatabaseName);
                    var urlRecordsMap = Database.BlobUrls.Query()
                        .Where(pf => pf.Equal(u => u.BlockKey.IterationKey, iterationKey))
                        .Where(pf => pf.In(
                            u => u.BlockKey.BlockId,
                            blocks.Select(b => b.BlockKey.BlockId)))
                        .GroupBy(u => u.BlockKey.BlockId)
                        .ToImmutableDictionary(g => g.Key, g => g.ToImmutableArray());
                    var tasks = blocks
                        .Select(b => QueueIngestBlockAsync(
                            b,
                            urlRecordsMap[b.BlockKey.BlockId],
                            ingestClient,
                            tempTable.TempTableName,
                            ct))
                        .ToImmutableArray();

                    await TaskHelper.WhenAllWithErrors(tasks);

                    return true;
                }
            }

            //  No blocks or temp table was available
            return false;
        }

        private async Task QueueIngestBlockAsync(
            BlockRecord block,
            IEnumerable<BlobUrlRecord> urlRecords,
            IngestClient ingestClient,
            string tempTableName,
            CancellationToken ct)
        {
            var blockTag = $"drop-by:kusto-copy|{Guid.NewGuid()}";
            //  Get Uri with SAS tokens
            var authorizedUriTasks = urlRecords
                .Select(u => StagingBlobUriProvider.AuthorizeUriAsync(u.Url, ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(authorizedUriTasks);

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

            using (var tx = Database.CreateTransaction())
            {
                Database.Blocks.UpdateRecord(
                    block,
                    block with
                    {
                        State = BlockState.Queued,
                        BlockTag = blockTag
                    },
                    tx);
                Database.IngestionBatches.AppendRecords(ingestionBatches);

                tx.Complete();
            }
        }
    }
}