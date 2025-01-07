﻿using KustoCopyConsole.Entity.RowItems;
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

        public IImmutableDictionary<TableIdentity, SourceTableCache> SourceTableMap
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
                    if (sourceTableIteration.Destination != null)
                    {
                        var destination = sourceTableIteration.Destination;

                        yield return destination.RowItem;
                        foreach (var block in destination.BlockMap.Values)
                        {
                            yield return block.RowItem;
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

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendItemToCache(
            RowItemBase item)
        {
            switch (item)
            {
                case SourceTableRowItem st:
                    return AppendSourceTable(st);
                case SourceBlockRowItem sb:
                    return AppendSourceBlock(sb);
                case SourceUrlRowItem url:
                    return AppendSourceUrl(url);
                case DestinationTableRowItem dt:
                    return AppendDestinationTable(dt);
                case DestinationBlockRowItem db:
                    return AppendDestinationBlock(db);
                default:
                    throw new NotSupportedException(
                        $"Not supported row item type:  {item.GetType().Name}");
            }
        }

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendSourceTable(
            SourceTableRowItem item)
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
                            new SourceIterationCache(
                                item,
                                iteration.BlockMap, iteration.Destination)));
                }
                else
                {
                    return _sourceTableMap.SetItem(
                        tableId,
                        table.AppendIteration(new SourceIterationCache(item)));
                }
            }
            else
            {
                return _sourceTableMap.Add(tableId, new SourceTableCache(item));
            }
        }

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendSourceBlock(
            SourceBlockRowItem item)
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
                                    new SourceBlockCache(item, sourceBlock.UrlMap))));
                    }
                    else
                    {
                        return _sourceTableMap.SetItem(
                            tableId,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendBlock(new SourceBlockCache(item))));
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

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendSourceUrl(
            SourceUrlRowItem item)
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
                                    block.AppendUrl(new SourceUrlCache(item)))));
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

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendDestinationTable(
            DestinationTableRowItem item)
        {
            var tableId = item.SourceTable;

            if (_sourceTableMap.ContainsKey(tableId))
            {
                var sourceTable = _sourceTableMap[tableId];

                if (sourceTable.IterationMap.ContainsKey(item.IterationId))
                {
                    var sourceIteration = sourceTable.IterationMap[item.IterationId];
                    var destinationIteration = sourceIteration.Destination == null
                        ? new DestinationIterationCache(item)
                        : new DestinationIterationCache(
                            item,
                            sourceIteration.Destination.BlockMap);

                    return _sourceTableMap.SetItem(
                        tableId,
                        sourceTable.AppendIteration(
                            sourceIteration.AppendDestination(destinationIteration)));
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

        private IImmutableDictionary<TableIdentity, SourceTableCache> AppendDestinationBlock(
            DestinationBlockRowItem item)
        {
            var tableId = item.SourceTable;

            if (_sourceTableMap.ContainsKey(tableId))
            {
                var sourceTable = _sourceTableMap[tableId];

                if (sourceTable.IterationMap.ContainsKey(item.IterationId))
                {
                    var sourceIteration = sourceTable.IterationMap[item.IterationId];

                    if (sourceIteration.Destination != null)
                    {
                        var destinationIteration = sourceIteration.Destination;

                        return _sourceTableMap.SetItem(
                            tableId,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendDestination(
                                    destinationIteration.AppendBlock(
                                        new DestinationBlockCache(item)))));
                    }
                    else
                    {
                        throw new NotSupportedException("Destination Iteration should come before block in logs");
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