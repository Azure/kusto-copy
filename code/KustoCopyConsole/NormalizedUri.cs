using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole
{
    internal static class NormalizedUri
    {
        public static Uri NormalizeUri(string uriText)
        {
            return new Uri(uriText.Trim(' ', '/').ToLower());
        }
    }
}