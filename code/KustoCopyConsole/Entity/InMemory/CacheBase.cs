namespace KustoCopyConsole.Entity.InMemory
{
    internal abstract class CacheBase
    {
        public CacheBase(RowItem rowItem)
        {
            RowItem = rowItem;
        }

        public RowItem RowItem { get; }
    }
}