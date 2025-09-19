using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class QueueIngestRunner : ActivityRunnerBase
    {

        public QueueIngestRunner(
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
                 TimeSpan.FromSeconds(5))
        {
        }

        public override async Task RunActivityAsync(string activityName, CancellationToken ct)
        {
            var destinationTable = Parameterization.Activities[activityName].Destination
                .GetTableIdentity();

            while (!IsActivityCompleted(activityName))
            {
                var block = Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, activityName))
                    .Where(pf => pf.Equal(b => b.State, BlockState.Exported))
                    .OrderBy(b => b.BlockKey.IterationId)
                    .ThenBy(b => b.BlockKey.BlockId)
                    .Take(1)
                    .FirstOrDefault();

                if (block != null)
                {
                    var tempTable = Database.TempTables.Query()
                        .Where(pf => pf.Equal(
                            t => t.IterationKey,
                            block.BlockKey.ToIterationKey()))
                        .FirstOrDefault();

                    //  It's possible, although unlikely, the temp table hasn't been created yet
                    //  If so, we'll process this block later
                    if (tempTable == null)
                    {
                        await SleepAsync(ct);
                    }
                    else
                    {
                        var ingestClient = DbClientFactory.GetIngestClient(
                            destinationTable.ClusterUri,
                            destinationTable.DatabaseName,
                            tempTable.TempTableName);

                        await QueueIngestBlockAsync(block, ingestClient, ct);
                    }
                }
                else
                {
                    await SleepAsync(ct);
                }
            }
        }

        private async Task QueueIngestBlockAsync(
            BlockRecord block,
            IngestClient ingestClient,
            CancellationToken ct)
        {
            var urlRecords = Database.BlobUrls.Query()
                .Where(pf => pf.Equal(u => u.BlockKey.ActivityName, block.BlockKey.ActivityName))
                .Where(pf => pf.Equal(u => u.BlockKey.IterationId, block.BlockKey.IterationId))
                .Where(pf => pf.Equal(u => u.BlockKey.BlockId, block.BlockKey.BlockId))
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
            CommitQueuedBlobs(
                queuingTasks.Select(o => o.Result),
                block with
                {
                    State = BlockState.Queued,
                    BlockTag = blockTag
                });

            Trace.TraceInformation($"Block {block.BlockKey}:  {urlRecords.Length} urls queued");
        }

        private async Task<BlobUrlRecord> QueueIngestUrlAsync(
            IngestClient ingestClient,
            BlobUrlRecord blobUrl,
            string blockTag,
            DateTime? creationTime,
            CancellationToken ct)
        {
            var authorizedUri = await StagingBlobUriProvider.AuthorizeUriAsync(blobUrl.Url, ct);
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

        private void CommitQueuedBlobs(IEnumerable<BlobUrlRecord> blobUrls, BlockRecord block)
        {
            using (var tx = Database.Database.CreateTransaction())
            {
                Database.BlobUrls.Query(tx)
                    .Where(pf => pf.MatchKeys(
                        blobUrls.First(),
                        u => u.BlockKey.ActivityName,
                        u => u.BlockKey.IterationId,
                        u => u.BlockKey.BlockId))
                    .Delete();
                Database.Blocks.Query(tx)
                    .Where(pf => pf.MatchKeys(
                        block,
                        b => b.BlockKey.ActivityName,
                        b => b.BlockKey.IterationId,
                        b => b.BlockKey.BlockId))
                    .Delete();

                Database.BlobUrls.AppendRecords(blobUrls, tx);
                Database.Blocks.AppendRecord(block, tx);
            }
        }
    }
}