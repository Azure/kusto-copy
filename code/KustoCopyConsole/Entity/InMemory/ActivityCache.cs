using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class ActivityCache
    {
        public ActivityCache(
            IImmutableDictionary<long, IterationCache> iterationMap)
        {
            IterationMap = iterationMap;
        }

        public ActivityCache(IterationRowItem iterationItem)
        {
            var iterationId = iterationItem.IterationId;

            IterationMap = ImmutableDictionary<long, IterationCache>
                .Empty
                .Add(iterationId, new IterationCache(iterationItem));
        }

        public IImmutableDictionary<long, IterationCache> IterationMap { get; }

        public ActivityCache AppendIteration(IterationCache iteration)
        {
            var iterationId = iteration.RowItem.IterationId;

            return new ActivityCache(IterationMap.SetItem(iterationId, iteration));
        }
    }
}