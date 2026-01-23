using Azure.Core;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace KustoCopyConsole.Entity
{
    internal class TrackDatabase : DatabaseContextBase
    {
        private const string ACTIVITY_TABLE = "Activity";
        private const string ITERATION_TABLE = "Iteration";
        private const string BLOCK_TABLE = "Block";
        private const string BLOCK_METRIC_TABLE = "BlockMetric";
        private const string TEMP_TABLE_TABLE = "TempTable";
        private const string BLOB_URL_TABLE = "BlobUrl";
        private const string INGESTION_BATCH_TABLE = "IngestionBatch";
        private const string EXTENT_TABLE = "Extent";

        #region Constructor
        public static async Task<TrackDatabase> CreateAsync(
            Uri blobFolderUri,
            TokenCredential credentials,
            CancellationToken ct)
        {
            var dbContext = await Database.CreateAsync(
                DatabasePolicy.Create(),
                //DatabasePolicy.Create(
                //    LogPolicy: LogPolicy.Create(
                //        StorageConfiguration: new StorageConfiguration(
                //            blobFolderUri,
                //            credentials,
                //            null))),
                db => new TrackDatabase(db),
                ct,
                TypedTableSchema<ActivityRecord>.FromConstructor(ACTIVITY_TABLE)
                .AddPrimaryKeyProperty(a => a.ActivityName),
                TypedTableSchema<IterationRecord>.FromConstructor(ITERATION_TABLE)
                .AddPrimaryKeyProperty(i => i.IterationKey),
                TypedTableSchema<BlockRecord>.FromConstructor(BLOCK_TABLE)
                .AddPrimaryKeyProperty(b => b.BlockKey)
                .AddTrigger((db, tx) =>
                {
                    var typedDb = (TrackDatabase)db;

                    ComputeBlockMetric(typedDb, tx);
                }),
                TypedTableSchema<BlockMetricRecord>.FromConstructor(BLOCK_METRIC_TABLE),
                TypedTableSchema<TempTableRecord>.FromConstructor(TEMP_TABLE_TABLE)
                .AddPrimaryKeyProperty(t => t.IterationKey),
                TypedTableSchema<BlobUrlRecord>.FromConstructor(BLOB_URL_TABLE)
                .AddPrimaryKeyProperty(b => b.BlockKey)
                .AddPrimaryKeyProperty(b => b.Url),
                TypedTableSchema<IngestionBatchRecord>.FromConstructor(INGESTION_BATCH_TABLE),
                TypedTableSchema<ExtentRecord>.FromConstructor(EXTENT_TABLE)
                .AddPrimaryKeyProperty(e => e.BlockKey)
                .AddPrimaryKeyProperty(e => e.ExtentId));

            return dbContext;
        }

        private TrackDatabase(Database database)
            : base(database)
        {
        }
        #endregion

        public TypedTable<ActivityRecord> Activities =>
            Database.GetTypedTable<ActivityRecord>(ACTIVITY_TABLE);

        public TypedTable<IterationRecord> Iterations =>
            Database.GetTypedTable<IterationRecord>(ITERATION_TABLE);

        public TypedTable<BlockRecord> Blocks =>
            Database.GetTypedTable<BlockRecord>(BLOCK_TABLE);

        public TypedTable<BlockMetricRecord> BlockMetrics =>
            Database.GetTypedTable<BlockMetricRecord>(BLOCK_METRIC_TABLE);

        public TypedTable<TempTableRecord> TempTables =>
            Database.GetTypedTable<TempTableRecord>(TEMP_TABLE_TABLE);

        public TypedTable<BlobUrlRecord> BlobUrls =>
            Database.GetTypedTable<BlobUrlRecord>(BLOB_URL_TABLE);

        public TypedTable<IngestionBatchRecord> IngestionBatches =>
            Database.GetTypedTable<IngestionBatchRecord>(INGESTION_BATCH_TABLE);

        public TypedTable<ExtentRecord> Extents =>
            Database.GetTypedTable<ExtentRecord>(EXTENT_TABLE);

        public IImmutableDictionary<BlockMetric, long> QueryAggregatedBlockMetrics(
            IterationKey iterationKey,
            TransactionContext tx)
        {
            //  Easier to separate for DEBUG
            var allMetrics = BlockMetrics.Query(tx)
                .Where(pf => pf.Equal(bm => bm.IterationKey, iterationKey));
            var aggregatedMetrics = allMetrics
                //  Ensure each metric has an entry
                .Concat(Enum.GetValues<BlockMetric>().Select(
                    m => new BlockMetricRecord(iterationKey, m, 0)))
                .AggregateBy(bm => bm.BlockMetric, (long)0, (sum, bm) => sum + bm.Value)
                .ToImmutableDictionary();

            return aggregatedMetrics;
        }

        private static void ComputeBlockMetric(TrackDatabase db, TransactionContext tx)
        {
            var newBlocks = db.Blocks.Query(tx)
                .WithinTransactionOnly();
            var deletedBlocks = db.Blocks.TombstonedWithinTransaction(tx);
            var newBlocksStateMetrics = newBlocks
                .Select(b => new BlockMetricRecord(
                    b.BlockKey.IterationKey,
                    ToBlockMetric(b.State),
                    1));
            var newBlocksExportedRowMetrics = newBlocks
                .Select(b => new BlockMetricRecord(
                    b.BlockKey.IterationKey,
                    BlockMetric.ExportedRowCount,
                    b.ExportedRowCount));
            var deletedStateMetrics = deletedBlocks
                .Select(b => new BlockMetricRecord(
                    b.BlockKey.IterationKey,
                    ToBlockMetric(b.State),
                    -1));
            var deletedExportedRowMetrics = deletedBlocks
                .Select(b => new BlockMetricRecord(
                    b.BlockKey.IterationKey,
                    BlockMetric.ExportedRowCount,
                    -b.ExportedRowCount));
            var newMetrics = newBlocksStateMetrics
                .Concat(newBlocksExportedRowMetrics)
                .Concat(deletedStateMetrics)
                .Concat(deletedExportedRowMetrics)
                .GroupBy(bm => new
                {
                    bm.IterationKey,
                    bm.BlockMetric
                })
                .Select(g => new BlockMetricRecord(
                    g.Key.IterationKey,
                    g.Key.BlockMetric,
                    g.Sum(bm => bm.Value)));

            db.BlockMetrics.AppendRecords(newMetrics, tx);
        }

        private static BlockMetric ToBlockMetric(BlockState state)
        {   //  Assume the state and metrics are in the same order and states appear first
            return (BlockMetric)((int)state);
        }
    }
}