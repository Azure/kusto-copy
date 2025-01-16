using KustoCopyConsole.Entity.RowItems.Keys;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class KustoPriority : IComparable<KustoPriority>
    {
        public KustoPriority(
            string? activityName = null,
            long? iterationId = null,
            long? blockId = null)
        {
            ActivityName = activityName;
            IterationId = iterationId;
            BlockId = blockId;
        }

        public KustoPriority(IterationKey key)
            : this(key.ActivityName, key.IterationId)
        {
        }

        public static KustoPriority HighestPriority { get; } = new KustoPriority();

        public string? ActivityName { get; }

        public long? IterationId { get; }

        public long? BlockId { get; }

        int IComparable<KustoPriority>.CompareTo(KustoPriority? other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            return CompareHierarchicalCompare(
                () => CompareStrings(ActivityName, other.ActivityName),
                () => CompareLongs(IterationId, other.IterationId),
                () => CompareLongs(BlockId, other.BlockId));
        }

        #region Compare primitives
        private int CompareStrings(string? a, string? b)
        {
            return (a == null && b == null)
                ? 0
                : (a == null && b != null)
                ? -1
                : (a != null && b == null)
                ? 1
                : a!.CompareTo(b!);
        }

        private static int CompareLongs(long? a, long? b)
        {
            return (a == null && b == null)
                ? 0
                : (a == null && b != null)
                ? -1
                : (a != null && b == null)
                ? 1
                : a!.Value.CompareTo(b!.Value);
        }

        private static int CompareHierarchicalCompare(params Func<int>[] compareValueEvaluators)
        {
            if (compareValueEvaluators.Length == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(compareValueEvaluators));
            }
            foreach (var evaluator in compareValueEvaluators)
            {
                var value = evaluator();

                if (value != 0)
                {
                    return value;
                }
            }

            return 0;
        }
        #endregion
    }
}