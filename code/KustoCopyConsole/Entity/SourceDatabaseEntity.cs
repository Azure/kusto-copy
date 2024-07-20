using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Entity
{
    internal class SourceDatabaseEntity : IterationEntityBase, IRowItemSerializable
    {
        public SourceDatabaseEntity(
            DatabaseReference sourceDatabase,
            long iterationId,
            DateTime creationTime,
            DateTime lastUpdateTime,
            SourceDatabaseState state,
            string? cursorStart,
            string cursorEnd)
            : base(creationTime, lastUpdateTime, sourceDatabase, iterationId)
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
            if (!string.IsNullOrWhiteSpace(item.SourceClusterUri)
                && !string.IsNullOrWhiteSpace(item.SourceDatabaseName)
                && item.IterationId != null
                && item.Created != null
                && item.Updated != null
                && !string.IsNullOrWhiteSpace(item.State)
                && !string.IsNullOrWhiteSpace(item.CursorEnd)
                && string.IsNullOrWhiteSpace(item.SourceTableName))
            {
                return new SourceDatabaseEntity(
                    new DatabaseReference(item.SourceClusterUri, item.SourceDatabaseName),
                    item.IterationId.Value,
                    item.Created.Value,
                    item.Updated.Value,
                    ParseEnum<SourceDatabaseState>(item.State),
                    item.CursorStart,
                    item.CursorEnd);
            }
            else
            {
                return null;
            }
        }

        RowItem IRowItemSerializable.Serialize()
        {
            throw new NotImplementedException();
        }
    }
}