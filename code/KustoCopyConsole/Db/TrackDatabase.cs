using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace KustoCopyConsole.Db
{
    internal class TrackDatabase : IAsyncDisposable
    {
        private const string ACTIVITY_TABLE = "Activity";
        private const string ITERATION_TABLE = "Iteration";
        private const string BLOCK_TABLE = "Block";

        #region Constructor
        public static async Task<TrackDatabase> CreateAsync()
        {
            var db = await Database.CreateAsync(
                new DatabasePolicies(),
                TypedTableSchema<ActivityRecord>.FromConstructor(ACTIVITY_TABLE),
                TypedTableSchema<IterationRecord>.FromConstructor(ITERATION_TABLE),
                TypedTableSchema<BlockRecord>.FromConstructor(BLOCK_TABLE));

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
    }
}