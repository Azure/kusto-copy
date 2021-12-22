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
    }
}