using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Parameters;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Text.Json;

namespace KustoCopyBookmarks.Export
{
    public class DbExportBookmark
    {
        #region Inner Types
        private class ExportAggregate
        {
            public DbIterationData? DbIteration { get; set; }

            public TableIterationData? TableIteration { get; set; }

            public EmptyTableExportEventData? EmptyTableExportEvent { get; set; }

            public TableExportEventData? TableExportEvent { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private BookmarkBlockValue<DbIterationData>? _backfillDbIteration;
        private BookmarkBlockValue<DbIterationData>? _forwardIteration;
        private ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>
            _backfillTableIterationMap;
        private ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>
            _forwardTableIterationMap;
        private ConcurrentQueue<BookmarkBlockValue<EmptyTableExportEventData>> _emptyTableExportEvents;

        public static async Task<DbExportBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential,
            Func<Task<(DbIterationData, IImmutableList<TableIterationData>)>> fetchDefaultContentAsync)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<ExportAggregate>();

            if (aggregates.Count() == 0)
            {
                return await CreateWithDefaultContentAsync(
                    fileClient,
                    bookmarkGateway,
                    fetchDefaultContentAsync);
            }
            else
            {
                return LoadAggregatesAsync(bookmarkGateway, aggregates);
            }
        }

        public async Task<IImmutableList<string>> ProcessEmptyTableAsync(
            bool isBackfill,
            Func<IEnumerable<string>, Task<IEnumerable<TableSchemaData>>> fetchSchemasAsync)
        {
            var map = isBackfill ? _backfillTableIterationMap : _forwardTableIterationMap;
            var dbIteration = isBackfill
                ? _backfillDbIteration!.Value
                : _backfillDbIteration!.Value;
            var emptyTableNames = map
                .Values
                .Select(t => t.Value)
                .Where(t => t.MinRemainingIngestionTime == null)
                .Select(t => t.TableName)
                .ToImmutableArray();
            var emptyTableIds = emptyTableNames
                .Select(n => map[n].BlockId);
            var schemas = await fetchSchemasAsync(emptyTableNames);
            var events = schemas
                .Zip(emptyTableNames, (s, n) => new { Schema = s, TableName = n })
                .Select(p => new EmptyTableExportEventData
                {
                    EndCursor = dbIteration.EndCursor,
                    EventTime = dbIteration.IterationTime,
                    TableName = p.TableName,
                    Schema = p.Schema
                });
            var eventBuffers = events
                .Select(e => SerializationHelper.ToMemory(
                    new ExportAggregate { EmptyTableExportEvent = e }));
            var transaction = new BookmarkTransaction(eventBuffers, null, emptyTableIds);
            //  Persist to blob
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);
            var eventValues = events
                .Zip(
                result.AddedBlockIds,
                (e, id) => new BookmarkBlockValue<EmptyTableExportEventData>(id, e));

            //  Persist to memory
            foreach (var tableName in emptyTableNames)
            {
                if (!map.TryRemove(tableName, out _))
                {
                    throw new InvalidOperationException(
                        $"Can't remove table '{tableName}' in memory with backfill={isBackfill}");
                }
            }
            foreach (var eventValue in eventValues)
            {
                _emptyTableExportEvents.Enqueue(eventValue);
            }

            return emptyTableNames;
        }

        private DbExportBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<DbIterationData>? backfillIteration,
            BookmarkBlockValue<DbIterationData>? forwardIteration,
            IEnumerable<BookmarkBlockValue<TableIterationData>> tableIterations,
            IEnumerable<BookmarkBlockValue<EmptyTableExportEventData>> emptyTableExportEvents)
        {
            _bookmarkGateway = bookmarkGateway;
            _backfillDbIteration = backfillIteration;
            _forwardIteration = forwardIteration;

            var backfillTableIterations = tableIterations
                .Where(t => t.Value.EndCursor == _backfillDbIteration?.Value.EndCursor)
                .Select(t => KeyValuePair.Create(t.Value.TableName, t));
            var forwardTableIterations = tableIterations
                .Where(t => t.Value.EndCursor == _forwardIteration?.Value.EndCursor)
                .Select(t => KeyValuePair.Create(t.Value.TableName, t));

            _backfillTableIterationMap = new ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>(backfillTableIterations);
            _forwardTableIterationMap = new ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>(forwardTableIterations);
            _emptyTableExportEvents = new ConcurrentQueue<BookmarkBlockValue<EmptyTableExportEventData>>(emptyTableExportEvents);
        }

        private static async Task<DbExportBookmark> CreateWithDefaultContentAsync(
            DataLakeFileClient fileClient,
            BookmarkGateway bookmarkGateway,
            Func<Task<(DbIterationData, IImmutableList<TableIterationData>)>> fetchDefaultContentAsync)
        {
            var (iterationDefinition, tables) = await fetchDefaultContentAsync();
            var iterationDefinitionBuffer = SerializationHelper.ToMemory(
                new ExportAggregate { DbIteration = iterationDefinition });
            var tableBuffers = tables.Select(t => SerializationHelper.ToMemory(
                    new ExportAggregate { TableIteration = t }));
            var transaction = new BookmarkTransaction(
                tableBuffers.Prepend(iterationDefinitionBuffer),
                null,
                null);
            var result = await bookmarkGateway.ApplyTransactionAsync(transaction);
            var backFillIteration = new BookmarkBlockValue<DbIterationData>(
                result.AddedBlockIds.First(),
                iterationDefinition);
            var tableValues = result.AddedBlockIds.Skip(1).Zip(
                tables,
                (r, t) => new BookmarkBlockValue<TableIterationData>(r, t));

            return new DbExportBookmark(
                bookmarkGateway,
                backFillIteration,
                null,
                tableValues,
                ImmutableArray<BookmarkBlockValue<EmptyTableExportEventData>>.Empty);
        }

        private static DbExportBookmark LoadAggregatesAsync(
            BookmarkGateway bookmarkGateway,
            IImmutableList<BookmarkBlockValue<ExportAggregate>> aggregates)
        {
            var iterations = aggregates
                .Where(a => a.Value.DbIteration != null);
            var backfillIterations = iterations
                .Where(a => a.Value.DbIteration!.StartCursor == null);
            var forwardIterations = iterations
                .Where(a => a.Value.DbIteration!.StartCursor != null);
            var tableIterations = aggregates
                .Where(a => a.Value.TableIteration != null);
            var emptyTableExportEvents = aggregates
                .Where(a => a.Value.EmptyTableExportEvent != null);

            if (backfillIterations.Count() > 1)
            {
                throw new InvalidOperationException(
                    "Expected at most one backfill iteration definition block");
            }
            if (forwardIterations.Count() > 1)
            {
                throw new InvalidOperationException(
                    "Expected at most one forward iteration definition block");
            }
            if (!iterations.Any())
            {
                throw new InvalidOperationException(
                    "Expected at least one iteration definition block");
            }
            var backfillIteration = backfillIterations.FirstOrDefault();
            var forwardIteration = forwardIterations.FirstOrDefault();

            return new DbExportBookmark(
                bookmarkGateway,
                backfillIteration?.Project(a => a.DbIteration!),
                forwardIteration?.Project(a => a.DbIteration!),
                tableIterations.Select(b => b.Project(a => a.TableIteration!)),
                emptyTableExportEvents.Select(e => e.Project(e => e.EmptyTableExportEvent!)));
        }
    }
}