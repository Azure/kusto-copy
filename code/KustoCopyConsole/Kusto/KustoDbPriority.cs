using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    public class KustoDbPriority : KustoPriorityBase, IComparable<KustoDbPriority>
    {
        public KustoDbPriority(
            long? iterationId = null,
            string? tableName = null,
            long? blockId = null)
        {
            IterationId = iterationId;
            TableName = tableName;
            BlockId = blockId;
        }

        public static KustoDbPriority HighestPriority { get; } = new KustoDbPriority();

        public long? IterationId { get; }

        public string? TableName { get; }

        public long? BlockId { get; }

        int IComparable<KustoDbPriority>.CompareTo(KustoDbPriority? other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            return CompareHierarchicalCompare(
                () => CompareLongs(IterationId, other.IterationId),
                () => CompareStrings(TableName, other.TableName),
                () => CompareLongs(BlockId, other.BlockId));
        }
    }
}