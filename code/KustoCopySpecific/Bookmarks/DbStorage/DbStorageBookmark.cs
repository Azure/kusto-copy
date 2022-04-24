using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyFoundation;
using KustoCopyFoundation.Bookmarks;
using KustoCopyFoundation.Concurrency;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Bookmarks.DbStorage
{
    public class DbStorageBookmark
    {
        #region Inner Types
        private class DbStorageAggregate
        {
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;

        public static async Task<DbStorageBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<DbStorageAggregate>();
            //var dbIterationValues = aggregates
            //    .Where(a => a.Value.DbIterations != null);
            //var dbIterations = dbIterationValues
            //    .Select(v => v.Value.DbIterations!)
            //    .SelectMany(i => i);

            return new DbStorageBookmark(bookmarkGateway);
        }

        private DbStorageBookmark(BookmarkGateway bookmarkGateway)
        {
            _bookmarkGateway = bookmarkGateway;
        }
    }
}