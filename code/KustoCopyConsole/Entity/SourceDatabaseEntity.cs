using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Entity
{
    internal class SourceDatabaseEntity : IterationEntityBase
    {
        public SourceDatabaseEntity(
            long iterationId,
            DatabaseReference sourceDatabase,
            DateTime creationTime,
            DateTime lastUpdateTime,
            SourceDatabaseState state,
            string? cursorStart,
            string cursorEnd)
            : base(iterationId, sourceDatabase, creationTime, lastUpdateTime)
        {
            State = state;
            CursorStart = cursorStart;
            CursorEnd = cursorEnd;
        }

        public SourceDatabaseState State { get; }

        /// <summary>This should be empty for-and-only-for the first iteration.</summary>
        public string? CursorStart { get; }

        public string CursorEnd { get; }

        public static SourceDatabaseEntity? Create(RowItem item)
        {
            throw new NotImplementedException();
        }
    }
}