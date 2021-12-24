using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace KustoCopyBookmarks.Export
{
    /// <summary>Represents a replication iteration at the table level.</summary>
    public class TableIterationData
    {
        /// <summary>Identify the iteration.</summary>
        public string EndCursor { get; set; } = string.Empty;

        public string TableName { get; set; } = string.Empty;

        public DateTime? MinRemainingIngestionTime { get; set; }

        public DateTime? MaxRemainingIngestionTime { get; set; }

        public (DateTime Min, DateTime Max) GetNextDayInterval(bool isBackfill)
        {
            if (MinRemainingIngestionTime == null)
            {
                throw new InvalidOperationException(
                    $"Can't replicate table '{TableName}' as there is no data left");
            }

            return isBackfill
                ? (MaxRemainingIngestionTime!.Value.Date, MaxRemainingIngestionTime.Value)
                : (MinRemainingIngestionTime!.Value, MinRemainingIngestionTime!.Value.Date.AddDays(1));
        }
    }
}