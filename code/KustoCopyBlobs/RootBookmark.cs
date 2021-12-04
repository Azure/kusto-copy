using Azure.Core;
using Azure.Storage.Files.DataLake;

namespace KustoCopyBlobs
{
    public class RootBookmark
    {
        private readonly BookmarkGateway _bookmarkGateway;

        internal static async Task<RootBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var blocks = await bookmarkGateway.ReadAllBlocksAsync();

            return new RootBookmark(bookmarkGateway);
        }

        private RootBookmark(BookmarkGateway bookmarkGateway)
        {
            _bookmarkGateway = bookmarkGateway;
        }

        public async Task<IAsyncDisposable> PermanentLockAsync()
        {
            return await _bookmarkGateway.PermanentLockAsync();
        }
    }
}