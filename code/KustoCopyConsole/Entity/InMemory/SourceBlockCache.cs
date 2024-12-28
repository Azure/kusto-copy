using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceBlockCache : CacheBase<SourceBlockRowItem>
    {
        public SourceBlockCache(
            SourceBlockRowItem item,
            IImmutableDictionary<string, SourceUrlCache> urlMap)
            : base(item)
        {
            UrlMap = urlMap;
        }

        public SourceBlockCache(SourceBlockRowItem item)
            : this(item, ImmutableDictionary<string, SourceUrlCache>.Empty)
        {
        }

        public IImmutableDictionary<string, SourceUrlCache> UrlMap { get; }

        public SourceBlockCache AppendUrl(SourceUrlCache url)
        {
            if (url.RowItem.State == SourceUrlState.Deleted)
            {
                if (UrlMap.ContainsKey(url.RowItem.Url))
                {
                    return new SourceBlockCache(
                        RowItem,
                        UrlMap.Remove(url.RowItem.Url));
                }
                else
                {
                    return this;
                }
            }
            else
            {
                return new SourceBlockCache(
                    RowItem,
                    UrlMap.SetItem(url.RowItem.Url, url));
            }
        }
    }
}