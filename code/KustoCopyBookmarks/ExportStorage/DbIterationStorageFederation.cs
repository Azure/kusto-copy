using Azure.Core;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.ExportStorage
{
    public class DbIterationStorageFederation
    {
        #region Inner types
        private class IterationNode
        {
        }
        #endregion

        private readonly DataLakeDirectoryClient _dbFolderClient;
        private readonly TokenCredential _credential;
        private readonly ConcurrentDictionary<DateTime, ConcurrentDictionary<int, IterationNode>> _nodeCache
            = new ConcurrentDictionary<DateTime, ConcurrentDictionary<int, IterationNode>>();

        public DbIterationStorageFederation(
            DataLakeDirectoryClient dbFolderClient,
            TokenCredential credential)
        {
            _dbFolderClient = dbFolderClient;
            _credential = credential;
        }

        public DataLakeDirectoryClient GetIterationFolder(
            bool isBackfill,
            DateTime epochStartTime,
            int iteration)
        {
            throw new NotImplementedException();
        }

        public Task<DbIterationStorageBookmark> FetchIterationBookmarkAsync(
            bool isBackfill,
            DateTime epochStartTime,
            int iteration)
        {
            throw new NotImplementedException();
        }
    }
}