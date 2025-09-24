using Azure.Core;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class QueueIngestRunner : ActivityRunnerBase
    {

        public QueueIngestRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(5))
        {
        }

        protected override async Task<bool> RunActivityAsync(
            string activityName,
            CancellationToken ct)
        {
            var destinationTable = RunnerParameters.Parameterization.Activities[activityName]
                .GetDestinationTableIdentity();
            var block = RunnerParameters.Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey.ActivityName, activityName))
                .Where(pf => pf.Equal(b => b.State, BlockState.Exported))
                .OrderBy(b => b.BlockKey.IterationKey.IterationId)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(1)
                .FirstOrDefault();

            if (block != null)
            {
                var tempTable = TryGetTempTable(block.BlockKey.IterationKey);

                //  It's possible, although unlikely, the temp table hasn't been created yet
                //  If so, we'll process this block later
                if (tempTable == null)
                {
                    return false;
                }
                else
                {
                    var ingestClient = RunnerParameters.DbClientFactory.GetIngestClient(
                        destinationTable.ClusterUri,
                        destinationTable.DatabaseName,
                        tempTable.TempTableName);

                    await QueueIngestBlockAsync(block, ingestClient, ct);

                    return true;
                }
            }
            else
            {
                return false;
            }
        }

        private async Task QueueIngestBlockAsync(
            BlockRecord block,
            IngestClient ingestClient,
            CancellationToken ct)
        {
            var urlRecords = RunnerParameters.Database.BlobUrls.Query()
                .Where(pf => pf.Equal(u => u.BlockKey, block.BlockKey))
                .ToImmutableArray();

            Trace.TraceInformation($"Block {block.BlockKey}:  ingest {urlRecords.Length} urls");

            var blockTag = $"drop-by:kusto-copy|{Guid.NewGuid()}";
            var queuingTasks = urlRecords
                .Select(u => QueueIngestUrlAsync(
                    ingestClient,
                    u,
                    blockTag,
                    block.MinCreationTime,
                    ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(queuingTasks);
            using (var tx = RunnerParameters.Database.Database.CreateTransaction())
            {
                var newBlobUrls = queuingTasks.Select(o => o.Result);

                RunnerParameters.Database.BlobUrls.Query(tx)
                    .Where(pf => pf.Equal(u => u.BlockKey, newBlobUrls.First().BlockKey))
                    .Delete();
                RunnerParameters.Database.BlobUrls.AppendRecords(newBlobUrls, tx);
                RunnerParameters.Database.Blocks.UpdateRecord(
                    block,
                    block with
                    {
                        State = BlockState.Queued,
                        BlockTag = blockTag
                    },
                    tx);

                tx.Complete();
            }

            Trace.TraceInformation($"Block {block.BlockKey}:  {urlRecords.Length} urls queued");
        }

        private async Task<BlobUrlRecord> QueueIngestUrlAsync(
            IngestClient ingestClient,
            BlobUrlRecord blobUrl,
            string blockTag,
            DateTime? creationTime,
            CancellationToken ct)
        {
            var authorizedUri = await RunnerParameters.StagingBlobUriProvider.AuthorizeUriAsync(blobUrl.Url, ct);
            var serializedQueueResult = await ingestClient.QueueBlobAsync(
                new KustoPriority(blobUrl.BlockKey),
                authorizedUri,
                blockTag,
                creationTime,
                ct);

            return blobUrl with
            {
                State = UrlState.Queued,
                SerializedQueuedResult = serializedQueueResult
            };
        }
    }
}
