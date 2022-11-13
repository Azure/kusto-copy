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
        public static string ExternalTableSchema => $"{nameof(IterationId)}:long, {nameof(EndCursor)}:string, {nameof(State)}:string, {nameof(Timestamp)}:datetime";

        #region Constructors
        public static StatusItem CreateIteration(long iterationId, string endCursor)
        {
            var item = new StatusItem
            {
                IterationId = iterationId,
                EndCursor = endCursor,
                State = StatusItemState.Initial,
                Timestamp = DateTime.UtcNow
            };

            return item;
        }

        public StatusItem()
        {
        }
        #endregion

        [Ignore]
        public HierarchyLevel Level => HierarchyLevel.Iteration;

        #region Data properties
        #region Iteration
        /// <summary>Identifier of the iteration.</summary>
        [Index(0)]
        public long IterationId { get; set; }

        /// <summary>End cursor of the iteration.</summary>
        [Index(1)]
        public string EndCursor { get; set; } = string.Empty;
        #endregion

        /// <summary>State of the item.</summary>
        [Index(2)]
        public StatusItemState State { get; set; } = StatusItemState.Initial;

        [Index(3)]
        public DateTime Timestamp { get; set; }
        #endregion
    }
}