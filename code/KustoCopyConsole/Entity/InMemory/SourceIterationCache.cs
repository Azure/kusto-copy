using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceIterationCache : CacheBase<SourceTableRowItem>
    {
        public SourceIterationCache(
            SourceTableRowItem item,
            IImmutableDictionary<long, SourceBlockCache> blockMap,
            IImmutableDictionary<TableIdentity, DestinationIterationCache> destinationMap)
            : base(item)
        {
            BlockMap = blockMap;
            DestinationMap = destinationMap;
        }

        public SourceIterationCache(SourceTableRowItem item)
            : this(
                  item,
                  ImmutableDictionary<long, SourceBlockCache>.Empty,
                  ImmutableDictionary<TableIdentity, DestinationIterationCache>.Empty)
        {
        }

        public IImmutableDictionary<long, SourceBlockCache> BlockMap { get; }

        public IImmutableDictionary<TableIdentity, DestinationIterationCache> DestinationMap { get; }

        public SourceIterationCache AppendBlock(SourceBlockCache block)
        {
            var newBlockMap = BlockMap.SetItem(block.RowItem.BlockId, block);

            return new SourceIterationCache(RowItem, newBlockMap, DestinationMap);
        }

        public SourceIterationCache AppendDestinationIteration(
            DestinationIterationCache destination)
        {
            var newDestinationMap =
                DestinationMap.SetItem(destination.RowItem.DestinationTable, destination);

            return new SourceIterationCache(RowItem, BlockMap, newDestinationMap);
        }
    }
}