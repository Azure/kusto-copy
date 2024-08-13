using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceTableIterationCache : CacheBase
    {
        public SourceTableIterationCache(RowItem item)
            : base(item)
        {
        }

        public SourceTableIterationCache Update(RowItem item)
        {
            return new SourceTableIterationCache(item);
        }
    }
}