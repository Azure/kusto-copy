using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    public abstract class KustoPriorityBase
    {
        protected int CompareStrings(string? a, string? b)
        {
            return (a == null && b == null)
                ? 0
                : (a == null && b != null)
                ? -1
                : (a != null && b == null)
                ? 1
                : a!.CompareTo(b!);
        }

        protected static int CompareLongs(long? a, long? b)
        {
            return (a == null && b == null)
                ? 0
                : (a == null && b != null)
                ? -1
                : (a != null && b == null)
                ? 1
                : a!.Value.CompareTo(b!.Value);
        }

        protected static int CompareHierarchicalCompare(params Func<int>[] compareValueEvaluators)
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
    }
}