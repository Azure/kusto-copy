using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Common
{
    public class TableSchemaData
    {
        public IImmutableList<ColumnSchemaData> Columns { get; set; } = ImmutableArray<ColumnSchemaData>.Empty;

        public string Folder { get; set; } = string.Empty;

        public string DocString { get; set; } = string.Empty;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as TableSchemaData;

            return other != null
                && Folder == other.Folder
                && DocString == other.DocString
                && Columns.SequenceEqual(other.Columns);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}