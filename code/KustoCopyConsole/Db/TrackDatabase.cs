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
        #region Constructor
        public static async Task<TrackDatabase> CreateAsync()
        {
            var db = Database.CreateAsync(
                new DatabasePolicies(),
                TypedTableSchema<>.FromConstructor());
        }

        private TrackDatabase(Database database)
        {
            Database = database;
        }
        #endregion

        public Database Database { get; }

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            throw new NotImplementedException();
        }
    }
}