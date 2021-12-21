using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Parameters;
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
            public IterationData? IterationDefinition { get; set; }

            public TableIngestionData? TableIngestionDays { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly BookmarkBlockValue<IterationData>? _backfillIteration;
        private readonly BookmarkBlockValue<IterationData>? _forwardIteration;
        private readonly IImmutableList<BookmarkBlockValue<TableIngestionData>> _backfillTables;

        public static async Task<DbExportBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential,
            Func<Task<(IterationData, IImmutableList<TableIngestionData>)>> fetchDefaultContentAsync)
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

            var tableNames = _backfillTables
                .Select(t => t.Value)
                .Where(t => !t.IngestionDayTime.Any())
                .Select(t => t.TableName)
                .ToImmutableArray();
            var schema = await fetchSchemaAsync(tableNames);

            return tableNames;
        }

        private DbExportBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<IterationData>? backfillIteration,
            BookmarkBlockValue<IterationData>? forwardIteration,
            IEnumerable<BookmarkBlockValue<TableIngestionData>> backfillTables)
        {
            _bookmarkGateway = bookmarkGateway;
            _backfillIteration = backfillIteration;
            _forwardIteration = forwardIteration;
            _backfillTables = backfillTables.ToImmutableArray();
        }

        private static async Task<DbExportBookmark> CreateWithDefaultContentAsync(
            DataLakeFileClient fileClient,
            BookmarkGateway bookmarkGateway,
            Func<Task<(IterationData, IImmutableList<TableIngestionData>)>> fetchDefaultContentAsync)
        {
            Trace.WriteLine($"Preparing {fileClient.Uri.PathAndQuery}...");

            var (iterationDefinition, tables) = await fetchDefaultContentAsync();
            var iterationDefinitionBuffer = SerializationHelper.ToMemory(
                new ExportAggregate { IterationDefinition = iterationDefinition });
            var tableBuffers = tables.Select(t => SerializationHelper.ToMemory(
                    new ExportAggregate { TableIngestionDays = t }));
            var transaction = new BookmarkTransaction(
                tableBuffers.Prepend(iterationDefinitionBuffer),
                null,
                null);
            var result = await bookmarkGateway.ApplyTransactionAsync(transaction);
            var backFillIteration = new BookmarkBlockValue<IterationData>(
                result.AddedBlockIds.First(),
                iterationDefinition);
            var tableValues = result.AddedBlockIds.Skip(1).Zip(
                tables,
                (r, t) => new BookmarkBlockValue<TableIngestionData>(r, t));

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
                .Where(a => a.Value.IterationDefinition != null);
            var backfillIterations = iterations
                .Where(a => a.Value.IterationDefinition!.StartCursor == null);
            var forwardIterations = iterations
                .Where(a => a.Value.IterationDefinition!.StartCursor != null);
            var tableIngestionDays = aggregates
                .Where(a => a.Value.TableIngestionDays != null);

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
                backfillIteration?.Project(a => a.IterationDefinition!),
                forwardIteration?.Project(a => a.IterationDefinition!),
                tableIngestionDays.Select(b => b.Project(a => a.TableIngestionDays!)));
        }
    }
}