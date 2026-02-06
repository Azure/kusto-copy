using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole
{
    internal static class EnumerableHelper
    {
        #region ArgMin + ArgMax
        public static T ArgExtremum<T, C>(
            this IEnumerable<T> enumerable,
            Func<C, C, int> comparer,
            Func<T, C> selector)
        {
            T? extremumItem = default;
            C? extremumValue = default;

            foreach (var item in enumerable)
            {
                if (extremumItem == null
                    || extremumValue == null
                    || comparer(selector(item), extremumValue) > 0)
                {
                    extremumItem = item;
                    extremumValue = selector(item);
                }
            }

            if (extremumItem == null)
            {
                throw new InvalidOperationException("Enumerable is empty");
            }

            return extremumItem;
        }

        public static T ArgMin<T>(this IEnumerable<T> enumerable, Func<T, long> selector)
        {
            return ArgExtremum(enumerable, (i1, i2) => -i1.CompareTo(i2), selector);
        }

        public static T ArgMax<T>(this IEnumerable<T> enumerable, Func<T, long> selector)
        {
            return ArgExtremum(enumerable, (i1, i2) => i1.CompareTo(i2), selector);
        }

        public static T ArgMin<T>(this IEnumerable<T> enumerable, Func<T, DateTime> selector)
        {
            return ArgExtremum(enumerable, (d1, d2) => -d1.CompareTo(d2), selector);
        }

        public static T ArgMax<T>(this IEnumerable<T> enumerable, Func<T, DateTime> selector)
        {
            return ArgExtremum(enumerable, (d1, d2) => d1.CompareTo(d2), selector);
        }
        #endregion

        public static IEnumerable<IEnumerable<T>> SplitInBatches<T>(
            this IEnumerable<T> enumerable,
            int batchSize)
        {
            if (batchSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(batchSize));
            }

            var currentBatch = ImmutableArray<T>.Empty.ToBuilder();

            foreach (var item in enumerable)
            {
                currentBatch.Add(item);
                if (currentBatch.Count >= batchSize)
                {
                    yield return currentBatch.ToImmutable();
                    currentBatch.Clear();
                }
            }
            if (currentBatch.Count > 0)
            {
                yield return currentBatch.ToImmutable();
            }
        }
    }
}