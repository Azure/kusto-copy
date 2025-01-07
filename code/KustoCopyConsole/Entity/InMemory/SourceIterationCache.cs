using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceIterationCache : CacheBase<SourceTableRowItem>
    {
        public SourceIterationCache(
            SourceTableRowItem item,
            IImmutableDictionary<long, SourceBlockCache> blockMap,
            DestinationIterationCache? destination)
            : base(item)
        {
            BlockMap = blockMap;
            Destination = destination;
        }

        public SourceIterationCache(SourceTableRowItem item)
            : this(
                  item,
                  ImmutableDictionary<long, SourceBlockCache>.Empty,
                  null)
        {
        }

        public IImmutableDictionary<long, SourceBlockCache> BlockMap { get; }

        public DestinationIterationCache? Destination { get; }

        public SourceIterationCache AppendBlock(SourceBlockCache block)
        {
            var newBlockMap = BlockMap.SetItem(block.RowItem.BlockId, block);

            return new SourceIterationCache(RowItem, newBlockMap, Destination);
        }

        public SourceIterationCache AppendDestination(DestinationIterationCache destination)
        {
            return new SourceIterationCache(RowItem, BlockMap, destination);
        }
    }
}