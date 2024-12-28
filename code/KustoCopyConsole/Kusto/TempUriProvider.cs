using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class TempUriProvider
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

        public async Task<Uri> FetchUriAsync(CancellationToken ct)
        {
            var innerCache = _innerCache;

            if (innerCache == null || innerCache.CacheTime < DateTime.Now - REFRESH_CACHE)
            {
                innerCache = new InnerCache(
                    DateTime.Now,
                    await _dmCommandClient.GetTempStorageUrisAsync(ct));

                Interlocked.Exchange(ref _innerCache, innerCache);
            }

            var tempUris = innerCache.TempUris;

            return tempUris[_random.Next(tempUris.Count)];
        }
    }
}