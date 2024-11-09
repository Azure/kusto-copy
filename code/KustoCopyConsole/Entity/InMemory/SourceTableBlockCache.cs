using KustoCopyConsole.Entity.RowItems;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class SourceTableBlockCache : CacheBase<SourceBlockRowItem>
    {
        public SourceTableBlockCache(SourceBlockRowItem item)
            : base(item)
        {
        }
    }
}