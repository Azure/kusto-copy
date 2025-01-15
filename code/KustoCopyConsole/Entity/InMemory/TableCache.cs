using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class TableCache
    {
        public TableCache(
            IImmutableDictionary<long, IterationCache> iterationMap)
        {
            IterationMap = iterationMap;
        }

        public TableCache(IterationRowItem iterationItem)
        {
            var iterationId = iterationItem.IterationId;

            IterationMap = ImmutableDictionary<long, IterationCache>
                .Empty
                .Add(iterationId, new IterationCache(iterationItem));
        }

        public IImmutableDictionary<long, IterationCache> IterationMap { get; }

        public TableCache AppendIteration(IterationCache iteration)
        {
            var iterationId = iteration.RowItem.IterationId;

            return new TableCache(IterationMap.SetItem(iterationId, iteration));
        }
    }
}