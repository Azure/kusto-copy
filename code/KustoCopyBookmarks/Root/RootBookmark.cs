using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Parameters;
using System.Collections.Immutable;
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

        public static async Task<RootBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var blocks = await bookmarkGateway.ReadAllBlocksAsync();
            var aggregates = blocks
                .Select(b => new BookmarkBlockValue<RootAggregate>(
                    b.Id,
                    SerializationHelper.ToObject<RootAggregate>(b.Buffer)))
                .ToImmutableArray();

            if (aggregates.Count() != 0)
            {
                var firstAggregate = aggregates.First();
                var parameterization = firstAggregate.Value.Parameterization;

                if (parameterization == null)
                {
                    throw new InvalidOperationException(
                        "Expected first block of root bookmark to be a parameterization");
                }
                var value = new BookmarkBlockValue<MainParameterization>(
                    firstAggregate.BlockId,
                    parameterization);

                return new RootBookmark(bookmarkGateway, value);
            }
            else
            {
                return new RootBookmark(bookmarkGateway, null);
            }
        }

        private RootBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<MainParameterization>? parameterization)
        {
            _bookmarkGateway = bookmarkGateway;
            _parameterization = parameterization;
        }

        public async Task SetParameterizationAsync(MainParameterization parameterization)
        {
            if (_parameterization != null)
            {
                throw new NotImplementedException();
            }

            var buffer = SerializationHelper.ToMemory(new RootAggregate { Parameterization = parameterization });
            var transaction = new BookmarkTransaction(new[] { buffer }, null, null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);

            _parameterization = new BookmarkBlockValue<MainParameterization>(result.AddedBlockIds.First(), parameterization);
        }
    }
}