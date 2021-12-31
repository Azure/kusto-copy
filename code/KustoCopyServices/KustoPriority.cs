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
        private readonly bool _isBackfill;
        private readonly DateTime _ingestionTime;
        private readonly KustoOperation _operation;

        public KustoPriority(bool isBackfill, DateTime ingestionTime, KustoOperation operation)
            : this(false, isBackfill, ingestionTime, operation)
        {
        }

        private KustoPriority(
            bool isWildCard,
            bool isBackfill,
            DateTime ingestionTime,
            KustoOperation operation)
        {
            _isWildcard = isWildCard;
            _isBackfill = isBackfill;
            _ingestionTime = ingestionTime;
            _operation = operation;
        }

        public static KustoPriority CreateNonPriority()
        {
            return new KustoPriority(true, false, DateTime.MinValue, KustoOperation.QueryOrCommand);
        }

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

                if (operationCompare != 0)
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