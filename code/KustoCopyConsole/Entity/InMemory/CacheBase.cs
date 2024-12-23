using KustoCopyConsole.Entity.RowItems;

namespace KustoCopyConsole.Entity.InMemory
{
    internal abstract class CacheBase<T>
        where T : RowItemBase
    {
        public CacheBase(T rowItem)
        {
            RowItem = rowItem;
        }

        public T RowItem { get; }
    }
}