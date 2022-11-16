using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class PlanRecordBatchState
    {
        public IImmutableList<TimeInterval> IngestionTimes { get; set; }
                = ImmutableArray<TimeInterval>.Empty;

        public DateTime? CreationTime { get; set; }

        public long? RecordCount { get; set; }
    }
}