using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class TableBookmark
    {
        public string TableName { get; set; } = "<EMPTY?>";

        public bool IsBackfill { get; set; } = true;

        public DateTime? MinTime { get; set; }
        
        public DateTime? MaxTime { get; set; }
        
        public DateTime? ExportedUntilTime { get; set; }
    }
}