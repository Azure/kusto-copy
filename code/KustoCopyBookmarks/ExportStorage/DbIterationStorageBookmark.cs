using Azure.Core;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.ExportStorage
{
    public class DbIterationStorageBookmark
    {
        #region Inner Types
        private class IterationAggregate
        {
            //public DbEpochData? DbEpoch { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        public static async Task<DbIterationStorageBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<IterationAggregate>();
            //var epochs = aggregates
            //    .Where(a => a.Value.DbEpoch != null);

            return new DbIterationStorageBookmark(
                bookmarkGateway);
        }

        private DbIterationStorageBookmark(
            BookmarkGateway bookmarkGateway)
        {
            _bookmarkGateway = bookmarkGateway;
        }
    }
}