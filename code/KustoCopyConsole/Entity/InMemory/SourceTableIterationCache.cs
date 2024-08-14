using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceTableIterationCache : CacheBase
    {
        private SourceTableIterationCache(
            RowItem item,
            IImmutableDictionary<long, SourceTableBlockCache> blockMap)
            : base(item)
        {
            BlockMap = blockMap;
        }

        public SourceTableIterationCache(RowItem item)
            : this(item, ImmutableDictionary<long, SourceTableBlockCache>.Empty)
        {
        }

        public IImmutableDictionary<long, SourceTableBlockCache> BlockMap { get; }

        public SourceTableIterationCache Update(RowItem iterationItem)
        {
            return new SourceTableIterationCache(iterationItem);
        }

        public SourceTableIterationCache AppendBlock(RowItem blockItem)
        {
            var newBlockMap = BlockMap.SetItem(
                blockItem.BlockId,
                new SourceTableBlockCache(blockItem));

            return new SourceTableIterationCache(RowItem, newBlockMap);
        }
    }
}