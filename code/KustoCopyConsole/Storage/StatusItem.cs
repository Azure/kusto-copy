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
        #region MyRegion
        /// <summary>Identifier of the iteration.</summary>
        [Index(0)]
        public long IterationId { get; set; }
        
        /// <summary>
        /// Start cursor of the iteration.  Is <c>null</c> on the first iteration (backfill).
        /// </summary>
        [Index(1)]
        public string? StartCursor { get; set; }

        /// <summary>End cursor of the iteration.</summary>
        [Index(2)]
        public string EndCursor { get; set; } = string.Empty;
        #endregion
    }
}