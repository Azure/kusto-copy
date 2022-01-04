using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    public class KustoPriority : IComparable<KustoPriority>
    {
        public KustoPriority(KustoOperation operation, bool isBackfill, DateTime ingestionTime)
            : this(false, operation, isBackfill, ingestionTime)
        {
        }

        private KustoPriority(
            bool isWildCard,
            KustoOperation operation,
            bool isBackfill,
            DateTime ingestionTime)
        {
            IsWildcard = isWildCard;
            IsBackfill = isBackfill;
            IngestionTime = ingestionTime;
            Operation = operation;
        }

        public static KustoPriority WildcardPriority { get; } =
            new KustoPriority(true, KustoOperation.QueryOrCommand, false, DateTime.MinValue);

        public static KustoPriority TerminateExportPriority { get; } =
            new KustoPriority(true, KustoOperation.TerminateExport, false, DateTime.MinValue);

        public bool IsWildcard { get; }
        
        public KustoOperation Operation { get; }
        
        public bool IsBackfill { get; }
        
        public DateTime IngestionTime { get; }

        int IComparable<KustoPriority>.CompareTo(KustoPriority? other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            if (IsWildcard && other.IsWildcard)
            {
                return 0;
            }
            else if (IsWildcard && !other.IsWildcard)
            {
                return -1;
            }
            else if (!IsWildcard && other.IsWildcard)
            {
                return 1;
            }
            else
            {
                var operationCompare = Operation.CompareTo(other.Operation);

                if (operationCompare != 0 || Operation == KustoOperation.TerminateExport)
                {
                    return operationCompare;
                }
                else if (IsBackfill && !other.IsBackfill)
                {
                    return 1;
                }
                else if (!IsBackfill && other.IsBackfill)
                {
                    return -1;
                }
                else
                {
                    return -IngestionTime.CompareTo(other.IngestionTime);
                }
            }
        }
    }
}