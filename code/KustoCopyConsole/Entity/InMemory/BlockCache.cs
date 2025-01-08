using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class BlockCache : CacheBase<BlockRowItem>
    {
        public BlockCache(BlockRowItem item, IImmutableDictionary<string, UrlCache> urlMap)
            : base(item)
        {
            UrlMap = urlMap;
        }

        public BlockCache(BlockRowItem item)
            : this(item, ImmutableDictionary<string, UrlCache>.Empty)
        {
        }

        public IImmutableDictionary<string, UrlCache> UrlMap { get; }

        public BlockCache AppendUrl(UrlCache url)
        {
            if (url.RowItem.State == UrlState.Deleted)
            {
                if (UrlMap.ContainsKey(url.RowItem.Url))
                {
                    return new BlockCache(
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
                return new BlockCache(
                    RowItem,
                    UrlMap.SetItem(url.RowItem.Url, url));
            }
        }
    }
}