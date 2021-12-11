using Azure.Core;
using Azure.Storage.Files.DataLake;
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
            public string? BackfillCursor { get; set; }

            public TableBookmark? TableBookmark { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly BookmarkBlockValue<string> _backfillCursor;
        private readonly IImmutableList<BookmarkBlockValue<TableBookmark>> _backfillTables;

        public string BackfillCursor => _backfillCursor.Value;

        public static async Task<DbExportBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential,
            Func<Task<(string, IImmutableList<TableBookmark>)>> fetchDefaultContentAsync)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<ExportAggregate>();

            if (aggregates.Count() == 0)
            {   //  Fill default content:  latest cursor as backfill cursor
                Trace.WriteLine($"Preparing {fileClient.Uri.PathAndQuery}...");

                var (latestCursor, tables) = await fetchDefaultContentAsync();
                var latestCursorBuffer = SerializationHelper.ToMemory(
                    new ExportAggregate { BackfillCursor = latestCursor });
                var tableBuffers = tables.Select(t => SerializationHelper.ToMemory(
                        new ExportAggregate { TableBookmark = t }));
                var transaction =
                    new BookmarkTransaction(tableBuffers.Prepend(latestCursorBuffer), null, null);
                var result = await bookmarkGateway.ApplyTransactionAsync(transaction);
                var backFillCursorvalue = new BookmarkBlockValue<string>(
                    result.AddedBlockIds.First(),
                    latestCursor);
                var tableValues = result.AddedBlockIds.Skip(1).Zip(
                    tables,
                    (r, t) => new BookmarkBlockValue<TableBookmark>(r, t));

                Trace.WriteLine($"{fileClient.Uri.PathAndQuery} is ready");
                
                return new DbExportBookmark(bookmarkGateway, backFillCursorvalue, tableValues);
            }
            else
            {
                var backfillCursors = aggregates.Where(a => a.Value.BackfillCursor != null);
                var backfillTables = aggregates.Where(a => a.Value.TableBookmark != null);

                if (!backfillCursors.Any())
                {
                    throw new InvalidOperationException("Expected a backfill cursor block");
                }
                if (backfillCursors.Count() > 1)
                {
                    throw new InvalidOperationException(
                        "Expected only one backfill cursor block "
                        + $"(got {backfillCursors.Count()})");
                }
                var backfillCursor = backfillCursors.First();
                var backfillCursorValue = new BookmarkBlockValue<string>(
                    backfillCursor.BlockId,
                    backfillCursor.Value.BackfillCursor!);
                var backfillTableValues = backfillTables
                    .Select(t => new BookmarkBlockValue<TableBookmark>(
                        t.BlockId,
                        t.Value.TableBookmark!));

                return new DbExportBookmark(
                    bookmarkGateway, backfillCursorValue, backfillTableValues);
            }
        }

        private DbExportBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<string> backfillCursor,
            IEnumerable<BookmarkBlockValue<TableBookmark>> backfillTables)
        {
            _bookmarkGateway = bookmarkGateway;
            _backfillCursor = backfillCursor;
            _backfillTables = backfillTables.ToImmutableArray();
        }
    }
}