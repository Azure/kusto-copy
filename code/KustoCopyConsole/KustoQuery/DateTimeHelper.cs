using Kusto.Data.Common;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public static class DateTimeHelper
    {
        public static string ToKql(this DateTime value)
        {
            var kql = CslDateTimeLiteral.AsCslString(value);

            return kql;
        }
    }
}