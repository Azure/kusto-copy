using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class IterationCache : CacheBase<IterationRowItem>
    {
        public IterationCache(
            IterationRowItem item,
            TempTableRowItem? tempTable,
            IImmutableDictionary<long, BlockCache> blockMap)
            : base(item)
        {
            TempTable = tempTable;
            BlockMap = blockMap;
            //  Clean everything after an iteration is completed
            if (item.State == IterationState.Completed)
            {
                TempTable = null;
                BlockMap = ImmutableDictionary<long, BlockCache>.Empty;
            }
        }

        public IterationCache(IterationRowItem item)
            : this(item, null, ImmutableDictionary<long, BlockCache>.Empty)
        {
        }

        public TempTableRowItem? TempTable { get; }

        public IImmutableDictionary<long, BlockCache> BlockMap { get; }

        public IterationCache CleanOnRestart()
        {
            var newBlockMap = BlockMap.Values
                .Select(b => b.CleanOnRestart())
                .ToImmutableDictionary(b => b.RowItem.BlockId);

            return new IterationCache(RowItem, TempTable, newBlockMap);
        }

        public IterationCache AppendBlock(BlockCache block)
        {
            var newBlockMap = BlockMap.SetItem(block.RowItem.BlockId, block);

            return new IterationCache(RowItem, TempTable, newBlockMap);
        }
    }
}