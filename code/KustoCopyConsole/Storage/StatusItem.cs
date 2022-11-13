using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    public class StatusItem
    {
        public static string ExternalTableSchema => $"{nameof(IterationId)}:long, {nameof(EndCursor)}:string, {nameof(Timestamp)}:datetime";

        #region MyRegion
        /// <summary>Identifier of the iteration.</summary>
        [Index(0)]
        public long IterationId { get; set; }

        /// <summary>End cursor of the iteration.</summary>
        [Index(1)]
        public string EndCursor { get; set; } = string.Empty;
        #endregion

        public DateTime Timestamp { get; set; }
    }
}