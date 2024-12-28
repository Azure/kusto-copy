using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Core.Tokens;

namespace KustoCopyConsole.Kusto
{
    public class KustoPriority : KustoPriorityBase, IComparable<KustoPriority>
    {
        public KustoPriority(string databaseName, KustoDbPriority kustoDbPriority)
        {
            DatabaseName = databaseName;
            KustoDbPriority = kustoDbPriority;
        }

        public static KustoPriority HighestPriority { get; } =
            new KustoPriority(string.Empty, KustoDbPriority.HighestPriority);

        public string DatabaseName { get; }

        public KustoDbPriority KustoDbPriority { get; }

        int IComparable<KustoPriority>.CompareTo(KustoPriority? other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            return CompareHierarchicalCompare(
                () => CompareLongs(KustoDbPriority.IterationId, KustoDbPriority.IterationId),
                () => CompareStrings(DatabaseName, other.DatabaseName),
                () => CompareStrings(KustoDbPriority.TableName, other.KustoDbPriority.TableName),
                () => CompareLongs(KustoDbPriority.BlockId, other.KustoDbPriority.BlockId));
        }
    }
}