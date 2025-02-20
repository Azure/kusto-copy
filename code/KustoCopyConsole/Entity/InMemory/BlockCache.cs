using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class BlockCache : CacheBase<BlockRowItem>
    {
        public BlockCache(
            BlockRowItem item,
            IImmutableDictionary<string, UrlCache> urlMap,
            IImmutableDictionary<string, ExtentCache> extentMap)
            : base(item)
        {
            UrlMap = urlMap;
            ExtentMap = extentMap;
            //  Removes urls when a block is sent back to planning
            if (item.State == BlockState.Planned)
            {
                UrlMap = ImmutableDictionary<string, UrlCache>.Empty;
            }
            //  Removes extents when a block is sent back to exported
            if (item.State == BlockState.Exported)
            {
                ExtentMap = ImmutableDictionary<string, ExtentCache>.Empty;
            }
            //  Removes all children when a block is moved
            if (item.State == BlockState.ExtentMoved)
            {
                UrlMap = ImmutableDictionary<string, UrlCache>.Empty;
                ExtentMap = ImmutableDictionary<string, ExtentCache>.Empty;
            }
        }

        public BlockCache(BlockRowItem item)
            : this(
                  item,
                  ImmutableDictionary<string, UrlCache>.Empty,
                  ImmutableDictionary<string, ExtentCache>.Empty)
        {
        }

        public IImmutableDictionary<string, UrlCache> UrlMap { get; }

        public IImmutableDictionary<string, ExtentCache> ExtentMap { get; }

        public BlockCache CleanOnRestart()
        {
            var newUrls = UrlMap.Values.AsEnumerable();
            var newExtents = ExtentMap.Values.AsEnumerable();

            //  A block was in the middle of exporting
            newUrls = RowItem.State == BlockState.Exporting
                ? Array.Empty<UrlCache>()
                : UrlMap.Values;

            //  A block was in the middle of being ingested
            newExtents = RowItem.State == BlockState.Queued
                ? Array.Empty<ExtentCache>()
                : ExtentMap.Values;

            //  Bring back url to exported
            if (RowItem.State == BlockState.Exported)
            {
                newUrls = newUrls
                    .Select(u =>
                    {
                        if (u.RowItem.State == UrlState.Queued)
                        {
                            var newUrl = u.RowItem.ChangeState(UrlState.Exported);

                            newUrl.SerializedQueuedResult = string.Empty;

                            return new UrlCache(newUrl);
                        }
                        else
                        {
                            return u;
                        }
                    });
            }

            return new BlockCache(
                RowItem,
                newUrls.ToImmutableDictionary(u => u.RowItem.Url),
                newExtents.ToImmutableDictionary(e => e.RowItem.ExtentId));
        }

        public BlockCache AppendUrl(UrlCache url)
        {
            return new BlockCache(
                RowItem,
                UrlMap.SetItem(url.RowItem.Url, url),
                ExtentMap);
        }

        public BlockCache AppendExtent(ExtentCache extent)
        {
            return new BlockCache(
                RowItem,
                UrlMap,
                ExtentMap.SetItem(extent.RowItem.ExtentId, extent));
        }
    }
}