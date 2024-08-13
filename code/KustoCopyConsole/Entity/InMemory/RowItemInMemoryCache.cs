using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class RowItemInMemoryCache
    {
        private readonly object _lock = new object();
        private volatile IImmutableDictionary<(Uri, string), SourceDatabaseCache> _sourceDatabaseMap
            = ImmutableDictionary<(Uri, string), SourceDatabaseCache>.Empty;

        public RowItemInMemoryCache(IEnumerable<RowItem> items)
        {
            lock (_lock)
            {
                foreach (var item in items)
                {
                    AddItem(item);
                }
            }
        }

        public IImmutableDictionary<(Uri, string), SourceDatabaseCache> SoureDatabaseMap
            => _sourceDatabaseMap;

        public IEnumerable<RowItem> GetItems()
        {
            throw new NotImplementedException();
        }

        public void AddItem(RowItem item)
        {
            lock (_lock)
            {
                if (item.RowType != RowType.FileVersion && item.RowType != RowType.Unspecified)
                {
                    Interlocked.Exchange(
                        ref _sourceDatabaseMap,
                        AddItemToCache(item));
                }
            }
        }

        private IImmutableDictionary<(Uri, string), SourceDatabaseCache> AddItemToCache(RowItem item)
        {
            switch (item.RowType)
            {
                case RowType.SourceDatabase:
                    return AddSourceDatabase(item);
                default:
                    throw new NotSupportedException($"Not supported row type:  {item.RowType}");
            }
        }

        private IImmutableDictionary<(Uri, string), SourceDatabaseCache> AddSourceDatabase(RowItem item)
        {
            var key = (NormalizedUri.NormalizeUri(item.SourceClusterUri), item.SourceDatabaseName);

            if (_sourceDatabaseMap.ContainsKey(key))
            {
                return _sourceDatabaseMap.SetItem(key, new SourceDatabaseCache(item));
            }
            else
            {
                return _sourceDatabaseMap.Add(key, new SourceDatabaseCache(item));
            }
        }
    }
}