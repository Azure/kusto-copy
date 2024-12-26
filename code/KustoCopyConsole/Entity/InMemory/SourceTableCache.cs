using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceTableCache
    {
        public SourceTableCache(
            IImmutableDictionary<long, SourceIterationCache> iterationMap)
        {
            IterationMap = iterationMap;
        }

        public SourceTableCache(SourceTableRowItem iterationItem)
        {
            var iterationId = iterationItem.IterationId;

            IterationMap = ImmutableDictionary<long, SourceIterationCache>
                .Empty
                .Add(iterationId, new SourceIterationCache(iterationItem));
        }

        public IImmutableDictionary<long, SourceIterationCache> IterationMap { get; }

        public SourceTableCache AppendIteration(SourceIterationCache iteration)
        {
            var iterationId = iteration.RowItem.IterationId;

            return new SourceTableCache(IterationMap.SetItem(iterationId, iteration));
        }
    }
}