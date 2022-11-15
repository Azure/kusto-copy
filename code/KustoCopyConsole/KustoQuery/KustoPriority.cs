using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public class KustoPriority : IComparable<KustoPriority>
    {
        public KustoPriority(
            long? iterationId = null,
            long? subIterationId = null,
            string? databaseName = null,
            string? tableName = null)
        {
            if (iterationId == null && subIterationId != null)
            {
                throw new ArgumentOutOfRangeException(nameof(subIterationId));
            }
            IterationId = iterationId;
            SubIterationId = subIterationId;
            DatabaseName = databaseName;
            TableName = tableName;
        }

        public long? IterationId { get; }

        public long? SubIterationId { get; }
        
        public string? DatabaseName { get; }
        
        public string? TableName { get; }

        int IComparable<KustoPriority>.CompareTo(KustoPriority? other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            return CompareHierarchicalCompare(
                CompareLongs(IterationId, other.IterationId),
                () => CompareLongs(SubIterationId, other.SubIterationId),
                () => CompareStrings(DatabaseName, other.DatabaseName),
                () => CompareStrings(TableName, other.TableName));
        }

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

        private static int CompareHierarchicalCompare(
            int initialCompareValue,
            params Func<int>[] subsequentCompareValueEvaluators)
        {
            if (initialCompareValue == 0)
            {
                return 0;
            }
            else
            {
                foreach (var evaluator in subsequentCompareValueEvaluators)
                {
                    var value = evaluator();

                    if (value != 0)
                    {
                        return value;
                    }
                }

                return 0;
            }
        }
    }
}