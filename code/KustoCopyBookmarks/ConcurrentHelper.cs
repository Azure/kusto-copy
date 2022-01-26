using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks
{
    public static class ConcurrentHelper
    {
        public static ConcurrentDictionary<K, V> ToConcurrentDictionary<K, V>(
            this IEnumerable<V> source,
            Func<V, K> keySelector) where K : notnull
        {
            var pairs = source
                .Select(e => KeyValuePair.Create(keySelector(e), e));

            return new ConcurrentDictionary<K, V>(pairs);
        }

        public static ConcurrentDictionary<K, V> ToConcurrentDictionary<K, V, T>(
            this IEnumerable<T> source,
            Func<T, K> keySelector,
            Func<T, V> elementSelector) where K : notnull
        {
            var pairs = source
                .Select(e => KeyValuePair.Create(keySelector(e), elementSelector(e)));

            return new ConcurrentDictionary<K, V>(pairs);
        }

        public static ConcurrentBag<T> ToConcurrentBag<T>(this IEnumerable<T> source)
        {
            return new ConcurrentBag<T>(source);
        }
    }
}