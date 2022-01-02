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
        private readonly bool _isWildcard;
        private readonly KustoOperation _operation;
        private readonly bool _isBackfill;
        private readonly DateTime _ingestionTime;

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
            _isWildcard = isWildCard;
            _isBackfill = isBackfill;
            _ingestionTime = ingestionTime;
            _operation = operation;
        }

        public static KustoPriority WildcardPriority { get; } =
            new KustoPriority(true, KustoOperation.QueryOrCommand, false, DateTime.MinValue);

        public static KustoPriority TerminateExportPriority { get; } =
            new KustoPriority(true, KustoOperation.TerminateExport, false, DateTime.MinValue);

        int IComparable<KustoPriority>.CompareTo(KustoPriority? other)
        {
            if (other == null)
            {
                throw new ArgumentNullException(nameof(other));
            }

            if (_isWildcard && other._isWildcard)
            {
                return 0;
            }
            else if (_isWildcard && !other._isWildcard)
            {
                return -1;
            }
            else if (!_isWildcard && other._isWildcard)
            {
                return 1;
            }
            else
            {
                var operationCompare = _operation.CompareTo(other._operation);

                if (operationCompare != 0 || _operation == KustoOperation.TerminateExport)
                {
                    return operationCompare;
                }
                else if (_isBackfill && !other._isBackfill)
                {
                    return 1;
                }
                else if (!_isBackfill && other._isBackfill)
                {
                    return -1;
                }
                else
                {
                    return -_ingestionTime.CompareTo(other._ingestionTime);
                }
            }
        }
    }
}