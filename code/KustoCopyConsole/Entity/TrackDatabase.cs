﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace KustoCopyConsole.Entity
{
    internal class TrackDatabase : IAsyncDisposable
    {
        private const string ACTIVITY_TABLE = "Activity";
        private const string ITERATION_TABLE = "Iteration";
        private const string BLOCK_TABLE = "Block";
        private const string TEMP_TABLE_TABLE = "TempTable";
        private const string BLOB_URL_TABLE = "BlobUrl";
        private const string INGESTION_BATCH_TABLE = "IngestionBatch";
        private const string EXTENT_TABLE = "Extent";

        #region Constructor
        public static async Task<TrackDatabase> CreateAsync()
        {
            var db = await Database.CreateAsync(
                new DatabasePolicies(),
                TypedTableSchema<ActivityRecord>.FromConstructor(ACTIVITY_TABLE)
                .AddPrimaryKeyProperty(a => a.ActivityName),
                TypedTableSchema<IterationRecord>.FromConstructor(ITERATION_TABLE)
                .AddPrimaryKeyProperty(i => i.IterationKey),
                TypedTableSchema<BlockRecord>.FromConstructor(BLOCK_TABLE)
                .AddPrimaryKeyProperty(b => b.BlockKey),
                TypedTableSchema<TempTableRecord>.FromConstructor(TEMP_TABLE_TABLE)
                .AddPrimaryKeyProperty(t => t.IterationKey),
                TypedTableSchema<BlobUrlRecord>.FromConstructor(BLOB_URL_TABLE)
                .AddPrimaryKeyProperty(b => b.BlockKey)
                .AddPrimaryKeyProperty(b => b.Url),
                TypedTableSchema<IngestionBatchRecord>.FromConstructor(INGESTION_BATCH_TABLE),
                TypedTableSchema<ExtentRecord>.FromConstructor(EXTENT_TABLE)
                .AddPrimaryKeyProperty(e => e.BlockKey)
                .AddPrimaryKeyProperty(e => e.ExtentId));

            return new TrackDatabase(db);
        }

        private TrackDatabase(Database database)
        {
            Database = database;
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
        }

        public Database Database { get; }

        public TypedTable<ActivityRecord> Activities =>
            Database.GetTypedTable<ActivityRecord>(ACTIVITY_TABLE);

        public TypedTable<IterationRecord> Iterations =>
            Database.GetTypedTable<IterationRecord>(ITERATION_TABLE);

        public TypedTable<BlockRecord> Blocks =>
            Database.GetTypedTable<BlockRecord>(BLOCK_TABLE);

        public TypedTable<TempTableRecord> TempTables =>
            Database.GetTypedTable<TempTableRecord>(TEMP_TABLE_TABLE);

        public TypedTable<BlobUrlRecord> BlobUrls =>
            Database.GetTypedTable<BlobUrlRecord>(BLOB_URL_TABLE);

        public TypedTable<IngestionBatchRecord> IngestionBatches =>
            Database.GetTypedTable<IngestionBatchRecord>(INGESTION_BATCH_TABLE);

        public TypedTable<ExtentRecord> Extents =>
            Database.GetTypedTable<ExtentRecord>(EXTENT_TABLE);
    }
}