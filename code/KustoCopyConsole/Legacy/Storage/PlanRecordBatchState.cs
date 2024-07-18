using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class PlanRecordBatchState
    {
        public IImmutableList<TimeInterval> IngestionTimes { get; set; }
                = ImmutableArray<TimeInterval>.Empty;

        public DateTime? CreationTime { get; set; }

        public long? RecordCount { get; set; }

        public PlanRecordBatchState Merge(PlanRecordBatchState other)
        {
            return new PlanRecordBatchState
            {
                IngestionTimes = IngestionTimes.AddRange(other.IngestionTimes),
                CreationTime = CreationTime,
                RecordCount = RecordCount + other.RecordCount
            };
        }

        public static IImmutableList<PlanRecordBatchState> Merge(
            IImmutableList<PlanRecordBatchState> batches,
            long maxMergeRecordCount,
            TimeSpan maxCreationTimeGap)
        {
            var mergedList = batches.Where(b => b.RecordCount >= maxMergeRecordCount).ToList();
            var toMergeList = batches
                .Where(b => b.RecordCount < maxMergeRecordCount)
                .OrderBy(b => b.CreationTime)
                .ToImmutableList();
            PlanRecordBatchState? currentBatch = null;

            foreach (var b in toMergeList)
            {
                if (currentBatch == null)
                {
                    currentBatch = b;
                }
                else
                {
                    if (b.CreationTime - currentBatch.CreationTime <= maxCreationTimeGap)
                    {
                        currentBatch = currentBatch.Merge(b);
                        if (currentBatch.RecordCount >= maxMergeRecordCount)
                        {
                            mergedList.Add(currentBatch);
                            currentBatch = null;
                        }
                    }
                    else
                    {
                        mergedList.Add(currentBatch);
                        currentBatch = b;
                    }
                }
            }
            if (currentBatch != null)
            {
                mergedList.Add(currentBatch);
            }

            return mergedList.ToImmutableList();
        }
    }
}