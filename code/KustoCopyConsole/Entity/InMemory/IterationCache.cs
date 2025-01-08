﻿using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class IterationCache : CacheBase<TableRowItem>
    {
        public IterationCache(
            TableRowItem item,
            IImmutableDictionary<long, BlockCache> blockMap)
            : base(item)
        {
            BlockMap = blockMap;
        }

        public IterationCache(TableRowItem item)
            : this(item, ImmutableDictionary<long, BlockCache>.Empty)
        {
        }

        public IImmutableDictionary<long, BlockCache> BlockMap { get; }

        public IterationCache AppendBlock(BlockCache block)
        {
            var newBlockMap = BlockMap.SetItem(block.RowItem.BlockId, block);

            return new IterationCache(RowItem, newBlockMap);
        }
    }
}