using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class BlockCache : CacheBase<BlockRowItem>
    {
        public BlockCache(BlockRowItem item, IImmutableDictionary<string, UrlCache> urlMap)
            : base(item)
        {   //  Removes urls when a block is sent back to prior state
            UrlMap = item.State <= BlockState.Exporting
                ? ImmutableDictionary<string, UrlCache>.Empty
                : urlMap;
        }

        public BlockCache(BlockRowItem item)
            : this(item, ImmutableDictionary<string, UrlCache>.Empty)
        {
        }

        public IImmutableDictionary<string, UrlCache> UrlMap { get; }

        public BlockCache CleanOnRestart()
        {   //  A block was in the middle of exporting
            var newUrls = RowItem.State == BlockState.Exporting
                ? Array.Empty<UrlCache>()
                : UrlMap.Values;

            //  Bring back url to exported
            if (RowItem.State == BlockState.Exported)
            {
                newUrls = newUrls
                    .Select(u =>
                    {
                        var newUrl = u.RowItem.ChangeState(UrlState.Exported);

                        newUrl.SerializedQueuedResult = string.Empty;

                        return new UrlCache(newUrl);
                    });
            }

            var newUrlMap = newUrls
                .ToImmutableDictionary(u => u.RowItem.Url);

            return new BlockCache(RowItem, newUrlMap);
        }

        public BlockCache AppendUrl(UrlCache url)
        {
            return new BlockCache(
                RowItem,
                UrlMap.SetItem(url.RowItem.Url, url));
        }
    }
}