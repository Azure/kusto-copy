using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class DestinationTableCache
    {
        public DestinationTableCache(
            IImmutableDictionary<long, DestinationIterationCache> iterationMap)
        {
            IterationMap = iterationMap;
        }

        public DestinationTableCache(DestinationTableRowItem iterationItem)
        {
            var iterationId = iterationItem.IterationId;

            IterationMap = ImmutableDictionary<long, DestinationIterationCache>
                .Empty
                .Add(iterationId, new DestinationIterationCache(iterationItem));
        }

        public IImmutableDictionary<long, DestinationIterationCache> IterationMap { get; }

        public DestinationTableCache AppendIteration(DestinationIterationCache iteration)
        {
            var iterationId = iteration.RowItem.IterationId;

            return new DestinationTableCache(IterationMap.SetItem(iterationId, iteration));
        }
    }
}