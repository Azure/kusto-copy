using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Parameters;
using System.Collections.Immutable;
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
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private BookmarkBlockValue<string> _backfillCursor;

        public string BackfillCursor => _backfillCursor.Value;

        public static async Task<DbExportBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential,
            Func<Task<string>> fetchLatestCursorAsync)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<ExportAggregate>();

            if (aggregates.Count() == 0)
            {   //  Fill default content:  latest cursor as backfill cursor
                var latestCursor = await fetchLatestCursorAsync();
                var buffer = SerializationHelper.ToMemory(
                    new ExportAggregate { BackfillCursor = latestCursor });
                var transaction = new BookmarkTransaction(new[] { buffer }, null, null);
                var result = await bookmarkGateway.ApplyTransactionAsync(transaction);
                var value = new BookmarkBlockValue<string>(
                    result.AddedBlockIds.First(),
                    latestCursor);

                return new DbExportBookmark(bookmarkGateway, value);
            }
            else
            {
                var firstAggregate = aggregates.First();
                var backfillCursor = firstAggregate.Value.BackfillCursor;

                if (backfillCursor == null)
                {
                    throw new InvalidOperationException(
                        "Expected first block of export bookmark to be backfill cursor");
                }
                var backfillCursorValue = new BookmarkBlockValue<string>(
                    firstAggregate.BlockId,
                    backfillCursor);

                return new DbExportBookmark(bookmarkGateway, backfillCursorValue);
            }
        }

        private DbExportBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<string> backfillCursor)
        {
            _bookmarkGateway = bookmarkGateway;
            _backfillCursor = backfillCursor;
        }
    }
}