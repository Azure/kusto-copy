using KustoCopyConsole.Entity.RowItems;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class ExtentCache : CacheBase<ExtentRowItem>
    {
        public ExtentCache(ExtentRowItem item)
            : base(item)
        {
        }
    }
}