using KustoCopyConsole.Entity.RowItems;
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
        private volatile IImmutableDictionary<TableIdentity, TableCache> _sourceTableMap =
            ImmutableDictionary<TableIdentity, TableCache>.Empty;

        public RowItemInMemoryCache(IEnumerable<RowItemBase> items)
        {
            lock (_lock)
            {
                foreach (var item in items)
                {
                    AppendItem(item);
                }
            }
        }

        public IImmutableDictionary<TableIdentity, TableCache> SourceTableMap
            => _sourceTableMap;

        public IEnumerable<RowItemBase> GetItems()
        {
            foreach (var sourceTable in SourceTableMap.Values)
            {
                foreach (var sourceTableIteration in sourceTable.IterationMap.Values)
                {
                    yield return sourceTableIteration.RowItem;
                    foreach (var block in sourceTableIteration.BlockMap.Values)
                    {
                        yield return block.RowItem;
                        foreach (var url in block.UrlMap.Values)
                        {
                            yield return url.RowItem;
                        }
                    }
                }
            }
        }

        public void AppendItem(RowItemBase item)
        {
            lock (_lock)
            {
                Interlocked.Exchange(ref _sourceTableMap, AppendItemToCache(item));
            }
        }

        private IImmutableDictionary<TableIdentity, TableCache> AppendItemToCache(
            RowItemBase item)
        {
            switch (item)
            {
                case IterationRowItem st:
                    return AppendSourceTable(st);
                case BlockRowItem sb:
                    return AppendSourceBlock(sb);
                case UrlRowItem url:
                    return AppendSourceUrl(url);
                default:
                    throw new NotSupportedException(
                        $"Not supported row item type:  {item.GetType().Name}");
            }
        }

        private IImmutableDictionary<TableIdentity, TableCache> AppendSourceTable(
            IterationRowItem item)
        {
            var tableId = item.SourceTable;

            if (_sourceTableMap.ContainsKey(tableId))
            {
                var table = _sourceTableMap[tableId];

                if (table.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = table.IterationMap[item.IterationId];

                    return _sourceTableMap.SetItem(
                        tableId,
                        table.AppendIteration(
                            new IterationCache(item, iteration.BlockMap)));
                }
                else
                {
                    return _sourceTableMap.SetItem(
                        tableId,
                        table.AppendIteration(new IterationCache(item)));
                }
            }
            else
            {
                return _sourceTableMap.Add(tableId, new TableCache(item));
            }
        }

        private IImmutableDictionary<TableIdentity, TableCache> AppendSourceBlock(
            BlockRowItem item)
        {
            var tableId = item.SourceTable;

            if (_sourceTableMap.ContainsKey(tableId))
            {
                var sourceTable = _sourceTableMap[tableId];

                if (sourceTable.IterationMap.ContainsKey(item.IterationId))
                {
                    var sourceIteration = sourceTable.IterationMap[item.IterationId];

                    if (sourceIteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var sourceBlock = sourceIteration.BlockMap[item.BlockId];

                        return _sourceTableMap.SetItem(
                            tableId,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendBlock(
                                    new BlockCache(item, sourceBlock.UrlMap))));
                    }
                    else
                    {
                        return _sourceTableMap.SetItem(
                            tableId,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendBlock(new BlockCache(item))));
                    }
                }
                else
                {
                    throw new NotSupportedException("Iteration should come before block in logs");
                }
            }
            else
            {
                throw new NotSupportedException("Table should come before block in logs");
            }
        }

        private IImmutableDictionary<TableIdentity, TableCache> AppendSourceUrl(
            UrlRowItem item)
        {
            var tableId = item.SourceTable;

            if (_sourceTableMap.ContainsKey(tableId))
            {
                var sourceTable = _sourceTableMap[tableId];

                if (sourceTable.IterationMap.ContainsKey(item.IterationId))
                {
                    var sourceIteration = sourceTable.IterationMap[item.IterationId];

                    if (sourceIteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var block = sourceIteration.BlockMap[item.BlockId];

                        return _sourceTableMap.SetItem(
                            tableId,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendBlock(
                                    block.AppendUrl(new UrlCache(item)))));
                    }
                    else
                    {
                        throw new NotSupportedException("Block should come before url in logs");
                    }
                }
                else
                {
                    throw new NotSupportedException("Iteration should come before block in logs");
                }
            }
            else
            {
                throw new NotSupportedException("Table should come before block in logs");
            }
        }
    }
}