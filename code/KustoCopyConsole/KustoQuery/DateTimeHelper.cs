using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public static class DateTimeHelper
    {
        public static string ToKql(this DateTime d)
        {
            var kql =
                $"{d.Year:00}-{d.Month:00}-{d.Day:00}T{d.Hour:00}:{d.Minute:00}:{d.Second:00}.{d.Ticks % 10000000:0000000}Z";

            return kql;
        }
    }
}