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
            Func<IImmutableList<string>, Task<IImmutableDictionary<string, TableSchemaData>>> fetchSchemaAsync)
        {
            if (!isBackfill)
            {
                throw new NotImplementedException();
            }

            var emptyTableNames = _backfillTableIterationMap
                .Values
                .Select(t => t.Value)
                .Where(t => !t.RemainingDayIngestionTimes.Any())
                .Select(t => t.TableName)
                .ToImmutableArray();
            var schema = await fetchSchemaAsync(emptyTableNames);

            return emptyTableNames;
        }

        private DbExportBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<DbIterationData>? backfillIteration,
            BookmarkBlockValue<DbIterationData>? forwardIteration,
            IEnumerable<BookmarkBlockValue<TableIterationData>> tableIterations)
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
        }

        private static async Task<DbExportBookmark> CreateWithDefaultContentAsync(
            DataLakeFileClient fileClient,
            BookmarkGateway bookmarkGateway,
            Func<Task<(DbIterationData, IImmutableList<TableIterationData>)>> fetchDefaultContentAsync)
        {
            Trace.WriteLine($"Preparing {fileClient.Uri.PathAndQuery}...");

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

            Trace.WriteLine($"{fileClient.Uri.PathAndQuery} is ready");

            return new DbExportBookmark(
                bookmarkGateway,
                backFillIteration,
                null,
                tableValues);
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
                tableIterations.Select(b => b.Project(a => a.TableIteration!)));
        }
    }
}