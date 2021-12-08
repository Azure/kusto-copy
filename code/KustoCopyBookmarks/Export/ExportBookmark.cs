using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Parameters;
using System.Collections.Immutable;
using System.Reflection.Metadata;
using System.Text.Json;

namespace KustoCopyBookmarks.Export
{
    public class ExportBookmark
    {
        #region Inner Types
        private class ExportAggregate
        {
            public bool? IsBackfill { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private BookmarkBlockValue<bool>? _isBackfill;

        public bool? IsBackfill => _isBackfill?.Value;

        public static async Task<ExportBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<ExportAggregate>();

            if (aggregates.Count() != 0)
            {
                var firstAggregate = aggregates.First();
                var isBackfill = firstAggregate.Value.IsBackfill;

                if (isBackfill == null)
                {
                    throw new InvalidOperationException(
                        "Expected first block of export bookmark to be export state");
                }
                var isBackfillValue = new BookmarkBlockValue<bool>(
                    firstAggregate.BlockId,
                    isBackfill.Value);

                return new ExportBookmark(bookmarkGateway, isBackfillValue);
            }
            else
            {
                return new ExportBookmark(bookmarkGateway, null);
            }
        }

        private ExportBookmark(BookmarkGateway bookmarkGateway, BookmarkBlockValue<bool>? isBackfill)
        {
            _bookmarkGateway = bookmarkGateway;
            _isBackfill = isBackfill;
        }
    }
}