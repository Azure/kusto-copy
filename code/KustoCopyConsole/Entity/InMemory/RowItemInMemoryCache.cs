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
        private volatile IImmutableDictionary<TableIdentity, SourceTableCache> _sourceTableMap =
            ImmutableDictionary<TableIdentity, SourceTableCache>.Empty;

        public RowItemInMemoryCache(IEnumerable<RowItem> items)
        {
            lock (_lock)
            {
                foreach (var item in items)
                {
                    AppendItem(item);
                }
            }
        }

        public IImmutableDictionary<TableIdentity, SourceTableCache> SourceTableMap
            => _sourceTableMap;

        public IEnumerable<RowItem> GetItems()
        {
            foreach (var sourceTable in SourceTableMap.Values)
            {
                foreach (var sourceTableIteration in sourceTable.IterationMap.Values)
                {
                    yield return sourceTableIteration.RowItem;
                }
            }
        }

        public void AppendItem(RowItem item)
        {
            lock (_lock)
            {
                if (item.RowType != RowType.FileVersion && item.RowType != RowType.Unspecified)
                {
                    Interlocked.Exchange(
                        ref _sourceTableMap,
                        AppendItemToCache(item));
                }
            }
        }

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendItemToCache(
            RowItem item)
        {
            switch (item.RowType)
            {
                case RowType.SourceTable:
                    return AppendSourceTable(item);
                default:
                    throw new NotSupportedException($"Not supported row type:  {item.RowType}");
            }
        }

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendSourceTable(
            RowItem item)
        {
                //item.IterationId!.Value
            var tableId = new TableIdentity(
                NormalizedUri.NormalizeUri(item.SourceClusterUri),
                item.SourceDatabaseName,
                item.SourceTableName);

            if (_sourceTableMap.ContainsKey(tableId))
            {
                return _sourceTableMap.SetItem(
                    tableId,
                    _sourceTableMap[tableId].AppendIteration(item));
            }
            else
            {
                return _sourceTableMap.Add(tableId, new SourceTableCache(item));
            }
        }
    }
}