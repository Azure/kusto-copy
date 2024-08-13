using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceTableCache
    {
        public SourceTableCache(
            IImmutableDictionary<long, SourceTableIterationCache> iterationMap)
        {
            IterationMap = iterationMap;
        }

        public SourceTableCache(RowItem iterationItem)
        {
            var iterationId = iterationItem.IterationId;

            IterationMap = ImmutableDictionary<long, SourceTableIterationCache>
                .Empty
                .Add(iterationId, new SourceTableIterationCache(iterationItem));
        }

        public IImmutableDictionary<long, SourceTableIterationCache> IterationMap { get; }

        public SourceTableCache AppendIteration(RowItem item)
        {
            var iterationId = item.IterationId;

            if (IterationMap.ContainsKey(iterationId))
            {
                return new SourceTableCache(
                    IterationMap.SetItem(iterationId, IterationMap[iterationId].Update(item)));
            }
            else
            {
                return new SourceTableCache(
                    IterationMap.Add(iterationId, new SourceTableIterationCache(item)));
            }
        }
    }
}