using KustoCopyBookmarks.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.IterationExportStorage
{
    public class TableStorageData
    {
        public string TableName { get; set; } = string.Empty;

        public TableSchemaData Schema { get; set; } = new TableSchemaData();

        public IImmutableList<TableStorageFolderData> Steps { get; set; } =
            ImmutableArray<TableStorageFolderData>.Empty;
    }
}