using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class UrlCache : CacheBase<UrlRowItem>
    {
        public UrlCache(UrlRowItem item)
            : base(item)
        {
        }
    }
}