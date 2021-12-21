using KustoCopyBookmarks.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class EmptyTableExportEventData
    {
        /// <summary>Identify the iteration.</summary>
        public string EndCursor { get; set; } = string.Empty;

        public string TableName { get; set; } = "<EMPTY?>";

        public TableSchemaData? Schema { get; set; }
}
}