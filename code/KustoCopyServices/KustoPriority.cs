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
        private KustoPriority(
            bool isWildCard,
            bool isExportRelated,
            DateTime ingestionTime)
        {
            IsWildcard = isWildCard;
            IsExportRelated = isExportRelated;
            IngestionTime = ingestionTime;
        }

        public static KustoPriority QueryPriority(DateTime ingestionTime)
        {
            return new KustoPriority(false, false, ingestionTime);
        }

        public static KustoPriority WildcardPriority { get; } =
            new KustoPriority(true, true, DateTime.MinValue);

        public static KustoPriority ExportPriority { get; } =
            new KustoPriority(false, true, DateTime.MinValue);

        public bool IsWildcard { get; }

        public bool IsExportRelated { get; }

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
            else if (IsExportRelated && other.IsExportRelated)
            {
                return 0;
            }
            else if (IsExportRelated && !other.IsExportRelated)
            {
                return -1;
            }
            else if (!IsExportRelated && other.IsExportRelated)
            {
                return 1;
            }
            else
            {
                return -IngestionTime.CompareTo(other.IngestionTime);
            }
        }
    }
}