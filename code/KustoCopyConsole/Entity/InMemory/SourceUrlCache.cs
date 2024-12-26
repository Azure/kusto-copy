using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceUrlCache : CacheBase<SourceUrlRowItem>
    {
        public SourceUrlCache(SourceUrlRowItem item)
            : base(item)
        {
        }
    }
}