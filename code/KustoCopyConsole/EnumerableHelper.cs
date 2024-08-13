using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole
{
    internal static class EnumerableHelper
    {
        public static T ArgMax<T>(this IEnumerable<T> enumerable, Func<T, long> selector)
        {
            T? maxItem = default;
            long maxValue = 0;

            foreach (var item in enumerable)
            {
                if (maxItem == null
                    || selector(item) > maxValue)
                {
                    maxItem = item;
                }
            }

            if (maxItem == null)
            {
                throw new InvalidOperationException("Enumerable is empty");
            }

            return maxItem;
        }
    }
}