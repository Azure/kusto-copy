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

        #region Constructor
        public static async Task<TrackDatabase> CreateAsync()
        {
            var db = await Database.CreateAsync(
                new DatabasePolicies(),
                TypedTableSchema<ActivityRecord>.FromConstructor(ACTIVITY_TABLE));

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

        public TypedTable<ActivityRecord> Activity =>
            Database.GetTypedTable<ActivityRecord>(ACTIVITY_TABLE);
    }
}