using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class DestinationBlockCache : CacheBase<DestinationBlockRowItem>
    {
        public DestinationBlockCache(DestinationBlockRowItem item)
            : base(item)
        {
        }
    }
}