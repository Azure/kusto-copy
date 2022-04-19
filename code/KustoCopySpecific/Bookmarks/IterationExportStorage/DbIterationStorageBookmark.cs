using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyFoundation;
using KustoCopyFoundation.Bookmarks;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Bookmarks.IterationExportStorage
{
    public class DbIterationStorageBookmark
    {
        #region Inner Types
        private class IterationAggregate
        {
            public DbIterationStorageData? DbIteration { get; set; }

            public TableStorageData? Table { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly ConcurrentDictionary<string, BookmarkBlockValue<TableStorageData>> _tables;
        private BookmarkBlockValue<DbIterationStorageData> _dbIteration;

        public static async Task<DbIterationStorageBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<IterationAggregate>();
            var dbIterations = aggregates
                .Where(a => a.Value.DbIteration != null);
            var dbIterationValue = dbIterations
                .Select(b => b.Project(a => a.DbIteration!))
                .FirstOrDefault();
            var tables = aggregates
                .Where(a => a.Value.Table != null);

            if (dbIterations.Count() > 1)
            {
                throw new InvalidOperationException(
                    "Expected at most one db iteration definition block");
            }
            if (dbIterationValue == null)
            {   //  Ensure a db iteration exist
                var dbIteration = new DbIterationStorageData();
                var dbIterationBuffer = SerializationHelper.ToMemory(
                    new IterationAggregate { DbIteration = dbIteration });
                var transaction = new BookmarkTransaction(
                    new[] { dbIterationBuffer },
                    null,
                    null);
                var result = await bookmarkGateway.ApplyTransactionAsync(transaction);

                dbIterationValue = new BookmarkBlockValue<DbIterationStorageData>(
                    result.AddedBlockIds.First(),
                    dbIteration);
            }

            return new DbIterationStorageBookmark(
                bookmarkGateway,
                dbIterationValue,
                tables.Select(b => b.Project(a => a.Table!)));
        }

        private DbIterationStorageBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<DbIterationStorageData> dbIteration,
            IEnumerable<BookmarkBlockValue<TableStorageData>> tables)
        {
            _bookmarkGateway = bookmarkGateway;
            _dbIteration = dbIteration;
            _tables = new ConcurrentDictionary<string, BookmarkBlockValue<TableStorageData>>(
                tables
                .Select(t => KeyValuePair.Create(t.Value.TableName, t)));
        }

        public DbIterationStorageData GetDbIteration()
        {
            return _dbIteration.Value;
        }

        public TableStorageData? GetTable(string tableName)
        {
            _tables.TryGetValue(tableName, out var table);

            return table?.Value;
        }

        public async Task CreateTableAsync(
            TableStorageData table,
            bool doReplaceTable,
            bool isIterationCompletelyExported)
        {
            if (!doReplaceTable && _tables.ContainsKey(table.TableName))
            {
                throw new InvalidOperationException(
                    $"A table block for '{table.TableName}' already exist");
            }
            if (isIterationCompletelyExported &&
                _dbIteration.Value.IsIterationCompletelyExported)
            {
                throw new InvalidOperationException("DB iteration is already sealed");
            }

            var newDbIteration = new DbIterationStorageData
            {
                IsIterationCompletelyExported = isIterationCompletelyExported
            };
            var tableBuffer = SerializationHelper.ToMemory(
                new IterationAggregate { Table = table });
            var addingBlockBuffers = !doReplaceTable ? new[] { tableBuffer } : null;
            var updatingBlocks = new List<BookmarkBlock>(2);

            if (doReplaceTable)
            {
                updatingBlocks.Add(new BookmarkBlock(_tables[table.TableName]!.BlockId, tableBuffer));
                _tables.TryRemove(table.TableName, out _);
            }
            if (isIterationCompletelyExported)
            {
                updatingBlocks.Add(new BookmarkBlock(
                    _dbIteration.BlockId,
                    SerializationHelper.ToMemory(newDbIteration)));
            }
            var transaction = new BookmarkTransaction(
                addingBlockBuffers,
                updatingBlocks,
                null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);
            var tableValue = new BookmarkBlockValue<TableStorageData>(
                doReplaceTable ? result.UpdatedBlockIds.First() : result.AddedBlockIds.First(),
                table);

            if (!_tables.TryAdd(table.TableName, tableValue))
            {
                throw new InvalidOperationException(
                    $"Two table blocks for '{table.TableName}' were inserted at once");
            }
            if (isIterationCompletelyExported)
            {
                var newId = result.UpdatedBlockIds.Last();

                _dbIteration = new BookmarkBlockValue<DbIterationStorageData>(
                    newId,
                    newDbIteration);
            }
        }
    }
}