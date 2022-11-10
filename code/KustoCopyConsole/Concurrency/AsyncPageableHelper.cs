using Azure;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Concurrency
{
    public static class AsyncPageableHelper
    {
        public static async Task<IImmutableList<T>> ToListAsync<T>(this AsyncPageable<T> pageable)
            where T : notnull
        {
            return await pageable.ToListAsync(t => t);
        }

        public static async Task<IImmutableList<V>> ToListAsync<T, V>(
            this AsyncPageable<T> pageable,
            Func<T, V> transform)
            where T : notnull
        {
            var builder = ImmutableArray<V>.Empty.ToBuilder();

            await foreach (var i in pageable)
            {
                var transformedItem = transform(i);

                builder.Add(transformedItem);
            }

            return builder.ToImmutableList();
        }
    }
}