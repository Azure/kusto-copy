using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class DestinationIterationCache : CacheBase<DestinationTableRowItem>
    {
        public DestinationIterationCache(
            DestinationTableRowItem item,
            IImmutableDictionary<long, DestinationBlockCache> blockMap)
            : base(item)
        {
            BlockMap = blockMap;
        }

        public DestinationIterationCache(DestinationTableRowItem item)
            : this(item, ImmutableDictionary<long, DestinationBlockCache>.Empty)
        {
        }

        public IImmutableDictionary<long, DestinationBlockCache> BlockMap { get; }

        public DestinationIterationCache AppendBlock(DestinationBlockCache block)
        {
            var newBlockMap = BlockMap.SetItem(block.RowItem.BlockId, block);

            return new DestinationIterationCache(RowItem, newBlockMap);
        }
    }
}