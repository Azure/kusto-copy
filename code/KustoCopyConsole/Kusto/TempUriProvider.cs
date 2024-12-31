using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class TempUriProvider : IBlobPathProvider
    {
        #region Inner Types
        private record InnerCache(DateTime CacheTime, IImmutableList<Uri> TempUris);
        #endregion

        private static readonly TimeSpan REFRESH_CACHE = TimeSpan.FromMinutes(10);

        private readonly DmCommandClient _dmCommandClient;
        private readonly Random _random = new Random();
        private volatile InnerCache? _innerCache;

        public TempUriProvider(DmCommandClient dmCommandClient)
        {
            _dmCommandClient = dmCommandClient;
        }

        async Task<Uri> IBlobPathProvider.FetchUriAsync(CancellationToken ct)
        {
            var innerCache = await GetOrFetchCacheAsync(ct);
            var tempUris = innerCache.TempUris;

            return tempUris[_random.Next(tempUris.Count)];
        }

        async Task<Uri> IBlobPathProvider.AuthorizeUriAsync(Uri uri, CancellationToken ct)
        {
            var innerCache = await GetOrFetchCacheAsync(ct);
            var queryString = innerCache.TempUris
                .Where(u => uri.ToString().StartsWith(RemoveQueryString(u).ToString()))
                .Select(t => t.Query)
                .FirstOrDefault();

            if (queryString == null)
            {
                throw new CopyException($"Can't find container for '{uri}'", false);
            }

            var builder = new UriBuilder(uri);

            builder.Query = queryString;

            return builder.Uri;
        }

        private static Uri RemoveQueryString(Uri uri)
        {
            var builder = new UriBuilder(uri);

            builder.Query = string.Empty;

            return builder.Uri;
        }

        private async Task<InnerCache> GetOrFetchCacheAsync(CancellationToken ct)
        {
            var innerCache = _innerCache;

            if (innerCache == null || innerCache.CacheTime < DateTime.Now - REFRESH_CACHE)
            {
                innerCache = new InnerCache(
                    DateTime.Now,
                    await _dmCommandClient.GetTempStorageUrisAsync(ct));

                Interlocked.Exchange(ref _innerCache, innerCache);
            }

            return innerCache;
        }
    }
}