using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceTableIterationCache : CacheBase<SourceTableRowItem>
    {
        private SourceTableIterationCache(
            SourceTableRowItem item,
            IImmutableDictionary<long, SourceTableBlockCache> blockMap)
            : base(item)
        {
            BlockMap = blockMap;
        }

        public SourceTableIterationCache(SourceTableRowItem item)
            : this(item, ImmutableDictionary<long, SourceTableBlockCache>.Empty)
        {
        }

        public IImmutableDictionary<long, SourceTableBlockCache> BlockMap { get; }

        public SourceTableIterationCache Update(SourceTableRowItem iterationItem)
        {
            return new SourceTableIterationCache(iterationItem);
        }

        public SourceTableIterationCache AppendBlock(SourceBlockRowItem blockItem)
        {
            var newBlockMap = BlockMap.SetItem(
                blockItem.BlockId,
                new SourceTableBlockCache(blockItem));

            return new SourceTableIterationCache(RowItem, newBlockMap);
        }
    }
}