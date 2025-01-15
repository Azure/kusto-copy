using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class ActivityCache : CacheBase<ActivityRowItem>
    {
        public ActivityCache(
            ActivityRowItem item,
            IImmutableDictionary<long, IterationCache> iterationMap)
            : base(item)
        {
            IterationMap = iterationMap;
        }

        public ActivityCache(ActivityRowItem item)
            : this(item, ImmutableDictionary<long, IterationCache>.Empty)
        {
        }

        public IImmutableDictionary<long, IterationCache> IterationMap { get; }

        public ActivityCache AppendIteration(IterationCache iteration)
        {
            var iterationId = iteration.RowItem.IterationId;

            return new ActivityCache(RowItem, IterationMap.SetItem(iterationId, iteration));
        }
    }
}