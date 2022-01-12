using Azure.Core;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
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
            private volatile Task<DbIterationStorageBookmark>? _retrieveTask;
            private volatile object _lastRead = DateTime.Now;
            private volatile DbIterationStorageBookmark? _bookmark;
            private volatile WeakReference<DbIterationStorageBookmark>? _bookmarkWeak;

            public IterationNode(Task<DbIterationStorageBookmark> bookmarkRetrieveTask)
            {
                _retrieveTask = bookmarkRetrieveTask;
            }

            public async Task<DbIterationStorageBookmark?> GetBookmarkAsync()
            {
                var task = _retrieveTask;

                Interlocked.Exchange(ref _lastRead, DateTime.Now);
                if (task != null)
                {
                    var bookmark = await task;

                    InterlockedSetBookmark(bookmark);
#pragma warning disable 4014
                    Interlocked.CompareExchange(ref _retrieveTask, task, null);
#pragma warning restore 4014

                    return bookmark;
                }
                else
                {
                    var weak = _bookmarkWeak;

                    if (weak == null)
                    {
                        throw new InvalidOperationException(
                            "Weak reference shouldn't be null at this point");
                    }
                    if (weak.TryGetTarget(out var bookmark))
                    {
                        InterlockedSetBookmark(bookmark);

                        return bookmark;
                    }
                    else
                    {
                        return null;
                    }
                }
            }

            public bool IsWeakReferenceOut()
            {
                var weak = _bookmarkWeak;

                if (weak != null)
                {
                    var isOut = !weak.TryGetTarget(out _);

                    return isOut;
                }

                //  Reference hasn't been created yet
                return false;
            }

            public void FlushOldHardReference()
            {
                var lastRead = (DateTime)_lastRead;

                if (lastRead < DateTime.Now.Subtract(CACHE_DURATION))
                {
                    Interlocked.Exchange(ref _bookmark, null);
                }
            }

            private void InterlockedSetBookmark(DbIterationStorageBookmark bookmark)
            {
                //  Use of Interlocked in case this runs of 32 bits architecture
                Interlocked.CompareExchange(ref _bookmark, null, bookmark);
                Interlocked.CompareExchange(
                    ref _bookmarkWeak,
                    null,
                    new WeakReference<DbIterationStorageBookmark>(bookmark));
            }
        }
        #endregion

        private static readonly TimeSpan CACHE_DURATION = TimeSpan.FromMinutes(1);

        private readonly DataLakeDirectoryClient _dbFolderClient;
        private readonly TokenCredential _credential;
        private readonly IDictionary<(DateTime startTime, int iteration), IterationNode> _cache =
            new Dictionary<(DateTime startTime, int iteration), IterationNode>();
        private readonly ReaderWriterLockSlim _lock =
            new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private volatile object _lastVacuum = DateTime.Now;

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
            var mainFolderClient = _dbFolderClient.GetSubDirectoryClient(
                isBackfill
                ? "backfill"
                : "forward");
            var epochFolderClient = mainFolderClient
                .GetSubDirectoryClient(epochStartTime.Year.ToString())
                .GetSubDirectoryClient($"{epochStartTime.Month:00}-{epochStartTime.Day:00}")
                .GetSubDirectoryClient($"{epochStartTime.Hour:00}:{epochStartTime.Minute:00}:{epochStartTime.Second:00}");
            var iterationFolderClient = epochFolderClient
                .GetSubDirectoryClient(iteration.ToString("000000"));

            return iterationFolderClient;
        }

        public async Task<DbIterationStorageBookmark> FetchIterationBookmarkAsync(
            bool isBackfill,
            DateTime epochStartTime,
            int iteration)
        {
            var cacheKey = (epochStartTime, iteration);
            IterationNode? iterationNode;

            VacuumIfNeeded();
            _lock.EnterUpgradeableReadLock();
            try
            {
                if (!_cache.TryGetValue(cacheKey, out iterationNode))
                {
                    iterationNode = EnsureIterationNode(isBackfill, cacheKey);
                }
            }
            finally
            {
                _lock.ExitUpgradeableReadLock();
            }
            var bookmark = await iterationNode.GetBookmarkAsync();

            if (bookmark == null)
            {
                DropNode(cacheKey, iterationNode);

                //  Recurse to fix
                return await FetchIterationBookmarkAsync(isBackfill, epochStartTime, iteration);
            }
            else
            {
                return bookmark;
            }
        }

        private void VacuumIfNeeded()
        {
            var snapshotLastVacuum = _lastVacuum;
            var lastVacuum = (DateTime)snapshotLastVacuum;

            if (lastVacuum < DateTime.Now.Subtract(CACHE_DURATION / 2))
            {
                _lock.EnterWriteLock();
                try
                {
                    if (object.ReferenceEquals(snapshotLastVacuum, _lastVacuum))
                    {
                        _lastVacuum = DateTime.Now;
                        //  Perform vacuum
                        var pairs = _cache.ToImmutableArray();

                        foreach (var p in pairs)
                        {
                            var key = p.Key;
                            var node = p.Value;

                            node.FlushOldHardReference();
                            if (node.IsWeakReferenceOut())
                            {
                                _cache.Remove(key);
                            }
                        }
                    }
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }
        }

        private void DropNode(
            (DateTime epochStartTime, int iteration) cacheKey,
            IterationNode nodeToDrop)
        {
            _lock.EnterWriteLock();
            try
            {
                IterationNode? currentIterationNode;

                if (_cache.TryGetValue(cacheKey, out currentIterationNode))
                {
                    if (object.ReferenceEquals(nodeToDrop, currentIterationNode))
                    {
                        _cache.Remove(cacheKey);
                    }
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        private IterationNode EnsureIterationNode(
            bool isBackfill,
            (DateTime epochStartTime, int iteration) cacheKey)
        {
            _lock.EnterWriteLock();
            try
            {
                IterationNode? iterationNode;

                if (!_cache.TryGetValue(cacheKey, out iterationNode))
                {
                    var folder = GetIterationFolder(
                        isBackfill,
                        cacheKey.epochStartTime,
                        cacheKey.iteration);
                    var file = folder.GetFileClient("iteration-storage.bookmark");

                    iterationNode = new IterationNode(DbIterationStorageBookmark.RetrieveAsync(
                        file,
                        _credential));
                    _cache.Add(cacheKey, iterationNode);
                }

                return iterationNode;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
    }
}