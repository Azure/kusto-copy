using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Parameters;
using System.Reflection.Metadata;
using System.Text.Json;

namespace KustoCopyBookmarks.Root
{
    public class RootBookmark
    {
        #region Inner Types
        private class RootAggregate
        {
            public MainParameterization? Parameterization { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private BookmarkBlockValue<MainParameterization>? _parameterization;

        public MainParameterization? Parameterization => _parameterization?.Value;

        internal static async Task<RootBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var blocks = await bookmarkGateway.ReadAllBlocksAsync();

            if (blocks.Count != 0)
            {
                throw new NotImplementedException();
            }

            return new RootBookmark(bookmarkGateway);
        }

        public async Task SetParameterizationAsync(MainParameterization parameterization)
        {
            if (_parameterization != null)
            {
                throw new NotImplementedException();
            }

            var buffer = SerializationHelper.SerializeToMemory(new RootAggregate { Parameterization = parameterization });
            var transaction = new BookmarkTransaction(new[] { buffer }, null, null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);

            _parameterization = new BookmarkBlockValue<MainParameterization>(result.AddedBlockIds.First(), parameterization);
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