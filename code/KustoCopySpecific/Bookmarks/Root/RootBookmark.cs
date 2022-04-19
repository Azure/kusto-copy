using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyFoundation;
using KustoCopyFoundation.Bookmarks;
using KustoCopySpecific.Parameters;
using System.Collections.Immutable;
using System.Reflection.Metadata;
using System.Text.Json;

namespace KustoCopySpecific.Bookmarks.Root
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
        private BookmarkBlockValue<MainParameterization> _parameterization;

        public MainParameterization Parameterization => _parameterization.Value;

        public static async Task<RootBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential,
            MainParameterization newParameterization)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<RootAggregate>();

            if (aggregates.Count() == 0)
            {   //  Default bookmark with current parameterization
                var buffer = SerializationHelper.ToMemory(
                    new RootAggregate { Parameterization = newParameterization });
                var transaction = new BookmarkTransaction(new[] { buffer }, null, null);
                var result = await bookmarkGateway.ApplyTransactionAsync(transaction);
                var value = new BookmarkBlockValue<MainParameterization>(
                    result.AddedBlockIds.First(),
                    newParameterization);

                return new RootBookmark(bookmarkGateway, value);
            }
            else
            {
                var firstAggregate = aggregates.First();
                var currentParameterization = firstAggregate.Value.Parameterization;

                if (currentParameterization == null)
                {
                    throw new InvalidOperationException(
                        "Expected first block of root bookmark to be a parameterization");
                }
                var value = new BookmarkBlockValue<MainParameterization>(
                    firstAggregate.BlockId,
                    currentParameterization);

                return new RootBookmark(bookmarkGateway, value);
            }
        }

        private RootBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<MainParameterization> parameterization)
        {
            _bookmarkGateway = bookmarkGateway;
            _parameterization = parameterization;
        }
    }
}