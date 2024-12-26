using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceBlockCache : CacheBase<SourceBlockRowItem>
    {
        private SourceBlockCache(SourceBlockRowItem item, IImmutableList<SourceUrlCache> urls)
            : base(item)
        {
            Urls = urls;
        }

        public SourceBlockCache(SourceBlockRowItem item)
            : this(item, ImmutableArray<SourceUrlCache>.Empty)
        {
        }

        public IImmutableList<SourceUrlCache> Urls { get; }

        public SourceBlockCache AppendUrl(SourceUrlCache url)
        {
            var existingUrlCache = Urls
                .Where(u => u.RowItem.Url == url.RowItem.Url)
                .FirstOrDefault();

            if (existingUrlCache != null)
            {
                var newUrls = Urls.Remove(existingUrlCache);

                if (url.RowItem.State != SourceUrlState.Deleted)
                {
                    newUrls = newUrls.Add(url);
                }

                return new SourceBlockCache(RowItem, newUrls);
            }
            else
            {
                var newUrls = Urls.Add(url);

                return new SourceBlockCache(RowItem, newUrls);
            }
        }
    }
}