using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceIterationCache : CacheBase<SourceTableRowItem>
    {
        public SourceIterationCache(
            SourceTableRowItem item,
            IImmutableDictionary<long, SourceBlockCache> blockMap)
            : base(item)
        {
            BlockMap = blockMap;
        }

        public SourceIterationCache(SourceTableRowItem item)
            : this(item, ImmutableDictionary<long, SourceBlockCache>.Empty)
        {
        }

        public IImmutableDictionary<long, SourceBlockCache> BlockMap { get; }

        public SourceIterationCache AppendBlock(SourceBlockCache block)
        {
            var newBlockMap = BlockMap.SetItem(block.RowItem.BlockId, block);

            return new SourceIterationCache(RowItem, newBlockMap);
        }
    }
}