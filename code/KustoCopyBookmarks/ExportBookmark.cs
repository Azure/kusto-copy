using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Parameters;
using System.Collections.Immutable;
using System.Reflection.Metadata;
using System.Text.Json;

namespace KustoCopyBookmarks
{
    public class ExportBookmark
    {
        #region Inner Types
        private class ExportAggregate
        {
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;

        public static async Task<ExportBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<ExportAggregate>();

            if (aggregates.Count() != 0)
            {
                throw new NotImplementedException();
            }
            else
            {
                return new ExportBookmark(bookmarkGateway);
            }
        }

        private ExportBookmark(BookmarkGateway bookmarkGateway)
        {
            _bookmarkGateway = bookmarkGateway;
        }
    }
}