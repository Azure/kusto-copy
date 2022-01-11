using Azure.Core;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.ExportStorage
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
        private BookmarkBlockValue<DbIterationStorageData>? _dbIteration;

        public static async Task<DbIterationStorageBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<IterationAggregate>();
            var dbIterations = aggregates
                .Where(a => a.Value.DbIteration != null);
            var tables = aggregates
                .Where(a => a.Value.Table != null);

            if (dbIterations.Count() > 1)
            {
                throw new InvalidOperationException(
                    "Expected at one db iteration definition block");
            }

            return new DbIterationStorageBookmark(
                bookmarkGateway,
                dbIterations.Select(b => b.Project(a => a.DbIteration!)).FirstOrDefault(),
                tables.Select(b => b.Project(a => a.Table!)));
        }

        private DbIterationStorageBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<DbIterationStorageData>? dbIteration,
            IEnumerable<BookmarkBlockValue<TableStorageData>> tables)
        {
            _bookmarkGateway = bookmarkGateway;
            _dbIteration = dbIteration;
            _tables = new ConcurrentDictionary<string, BookmarkBlockValue<TableStorageData>>(
                tables
                .Select(t => KeyValuePair.Create(t.Value.TableName, t)));
        }

        public DbIterationStorageData? GetDbIteration()
        {
            return _dbIteration?.Value;
        }

        public async Task<DbIterationStorageData> CreateDbIterationAsync()
        {
            if (_dbIteration != null)
            {
                throw new InvalidOperationException("A DB iteration block is already present");
            }

            var dbIteration = new DbIterationStorageData();
            var dbIterationBuffer = SerializationHelper.ToMemory(
                new IterationAggregate { DbIteration = dbIteration });
            var transaction = new BookmarkTransaction(
                new[] { dbIterationBuffer },
                null,
                null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);

            _dbIteration = new BookmarkBlockValue<DbIterationStorageData>(
                result.AddedBlockIds.First(),
                dbIteration);

            return dbIteration;
        }

        public async Task SealDbIterationAsync()
        {
            if (_dbIteration == null)
            {
                throw new InvalidOperationException("No DB iteration block present");
            }

            //  Seal in-memory
            _dbIteration.Value.AllTablesExported = true;

            var dbIterationBuffer = SerializationHelper.ToMemory(_dbIteration);
            var transaction = new BookmarkTransaction(
                null,
                new[] { new BookmarkBlock(_dbIteration.BlockId, dbIterationBuffer) },
                null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);

            if (result.UpdatedBlockIds.Count != 1
                || result.UpdatedBlockIds.First() != _dbIteration.BlockId)
            {
                throw new InvalidOperationException("Inconsistency with db iteration update");
            }
        }

        public TableStorageData? GetTable(string tableName)
        {
            _tables.TryGetValue(tableName, out var table);

            return table?.Value;
        }

        public async Task CreateTableAsync(TableStorageData table)
        {
            if (_tables.ContainsKey(table.TableName))
            {
                throw new InvalidOperationException(
                    $"A table block for '{table.TableName}' already exist");
            }

            var tableBuffer = SerializationHelper.ToMemory(
                new IterationAggregate { Table = table });
            var transaction = new BookmarkTransaction(
                new[] { tableBuffer },
                null,
                null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);
            var tableValue = new BookmarkBlockValue<TableStorageData>(
                result.AddedBlockIds.First(),
                table);
            var legal = _tables.TryAdd(table.TableName, tableValue);

            if (!legal)
            {
                throw new InvalidOperationException(
                    $"Two table blocks for '{table.TableName}' were inserted at once");
            }
        }
    }
}