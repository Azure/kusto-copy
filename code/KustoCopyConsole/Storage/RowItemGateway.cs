using Azure.Storage.Queues.Models;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal class RowItemGateway : IAsyncDisposable
    {
        #region Inner Types
        private record QueueItem(
            DateTime enqueueTime,
            byte[] Buffer,
            RowItemInMemoryCache SnapshotCache,
            TaskCompletionSource? TaskSource);
        #endregion

        private static readonly Version CURRENT_FILE_VERSION = new Version(0, 0, 1, 0);
        private static readonly TimeSpan MIN_WAIT_PERIOD = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan FLUSH_PERIOD = TimeSpan.FromSeconds(5);

        private static readonly RowItemSerializer _rowItemSerializer = CreateRowItemSerializer();

        private readonly object _lock = new object();
        private readonly IAppendStorage _appendStorage;
        private readonly ConcurrentQueue<QueueItem> _queue = new();
        private readonly Task _backgroundTask;
        private readonly TaskCompletionSource _backgroundCompletedSource = new();
        private volatile RowItemInMemoryCache _inMemoryCache;

        #region Construction
        private RowItemGateway(
            IAppendStorage appendStorage,
            RowItemInMemoryCache cache,
            CancellationToken ct)
        {
            _appendStorage = appendStorage;
            _backgroundTask = Task.Run(() => BackgroundPersistanceAsync(ct));
            _inMemoryCache = cache;
        }

        private static RowItemSerializer CreateRowItemSerializer()
        {
            return new RowItemSerializer()
                .AddType<FileVersionRowItem>(RowType.FileVersion)
                .AddType<ActivityRowItem>(RowType.Activity)
                .AddType<IterationRowItem>(RowType.Iteration)
                .AddType<TempTableRowItem>(RowType.TempTable)
                .AddType<BlockRowItem>(RowType.Block)
                .AddType<UrlRowItem>(RowType.Url);
        }

        public static async Task<RowItemGateway> CreateAsync(
            IAppendStorage appendStorage,
            CancellationToken ct)
        {
            var readBuffer = await appendStorage.LoadAllAsync(ct);
            var allItems = _rowItemSerializer.Deserialize(readBuffer);

            if (allItems.Any())
            {
                var version = allItems.First() as FileVersionRowItem;

                if (version == null)
                {
                    throw new InvalidDataException("First row is expected to be a version row");
                }
                if (version.FileVersion != CURRENT_FILE_VERSION)
                {
                    throw new NotSupportedException(
                        $"Only support version is {CURRENT_FILE_VERSION}");
                }
                //  Validate all
                foreach (var item in allItems)
                {
                    item.Validate();
                }
                //  Remove file version from it
                allItems = allItems.RemoveAt(0);
            }

            var newVersionItem = new FileVersionRowItem
            {
                FileVersion = CURRENT_FILE_VERSION
            };
            var cache = new RowItemInMemoryCache(allItems);

            //  Re-write the logs by taking items "compressed" by the cache
            using (var tempMemoryStream = new MemoryStream())
            {
                _rowItemSerializer.Serialize(newVersionItem, tempMemoryStream);
                foreach (var item in cache.GetItems())
                {
                    _rowItemSerializer.Serialize(item, tempMemoryStream);
                }

                var writeBuffer = tempMemoryStream.ToArray();

                await appendStorage.AtomicReplaceAsync(writeBuffer, ct);

                return new RowItemGateway(appendStorage, cache, ct);
            }
        }
        #endregion

        public event EventHandler<RowItemBase>? RowItemAppended;

        public RowItemInMemoryCache InMemoryCache => _inMemoryCache;

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _backgroundCompletedSource.SetResult();
            await _backgroundTask;
            await _appendStorage.DisposeAsync();
        }

        public void Append(RowItemBase item)
        {
            AppendInternal(item, null);
        }

        public void Append(IEnumerable<RowItemBase> items)
        {
            foreach (var item in items)
            {
                Append(item);
            }
        }

        public Task AppendAndPersistAsync(RowItemBase item, CancellationToken ct)
        {
            var taskSource = new TaskCompletionSource();

            AppendInternal(item, taskSource);

            return taskSource.Task;
        }

        public async Task AppendAndPersistAsync(
            IEnumerable<RowItemBase> items,
            CancellationToken ct)
        {
            var materializedItems = items.ToImmutableArray();

            if (materializedItems.Any())
            {
                Append(materializedItems.Take(materializedItems.Count() - 1));
                await AppendAndPersistAsync(materializedItems.Last(), ct);
            }
        }

        private void AppendInternal(RowItemBase item, TaskCompletionSource? TaskSource)
        {
            item.Validate();

            var binaryItem = GetBytes(item);

            lock (_lock)
            {
                var newCache = _inMemoryCache.AppendItem(item);

                Interlocked.Exchange(ref _inMemoryCache, newCache);
                _queue.Enqueue(new QueueItem(DateTime.Now, binaryItem, newCache, TaskSource));
            }
            OnRowItemAppended(item);
        }

        private void OnRowItemAppended(RowItemBase item)
        {
            if (RowItemAppended != null)
            {
                RowItemAppended(this, item);
            }
        }

        private static byte[] GetBytes(RowItemBase item)
        {
            using (var stream = new MemoryStream())
            {
                _rowItemSerializer.Serialize(item, stream);

                return stream.ToArray();
            }
        }

        private async Task BackgroundPersistanceAsync(CancellationToken ct)
        {
            while (!_backgroundCompletedSource.Task.IsCompleted)
            {
                if (_queue.TryPeek(out var queueItem))
                {
                    var delta = DateTime.Now - queueItem.enqueueTime;
                    var waitTime = FLUSH_PERIOD - delta;

                    if (waitTime < MIN_WAIT_PERIOD)
                    {
                        if (_appendStorage.IsCompactionRequired)
                        {
                            await CompactAsync(ct);
                        }
                        else
                        {
                            await PersistBatchAsync(ct);
                        }
                    }
                    else
                    {   //  Wait for first item to age to about FLUSH_PERIOD
                        await Task.WhenAny(
                            Task.Delay(waitTime, ct),
                            _backgroundCompletedSource.Task);
                    }
                }
                else
                {   //  Wait for an element to pop in
                    await Task.WhenAny(
                        Task.Delay(FLUSH_PERIOD, ct),
                        _backgroundCompletedSource.Task);
                }
            }
        }

        private async Task CompactAsync(CancellationToken ct)
        {
            var sources = new List<TaskCompletionSource>();
            RowItemInMemoryCache? latestSnapshotCache = null;

            while (_queue.TryDequeue(out var queueItem))
            {
                latestSnapshotCache = queueItem.SnapshotCache;
            }
            if (latestSnapshotCache == null)
            {
                throw new InvalidOperationException(
                    $"{nameof(latestSnapshotCache)} should be set here");
            }
            using (var tempMemoryStream = new MemoryStream())
            {
                var newVersionItem = new FileVersionRowItem
                {
                    FileVersion = CURRENT_FILE_VERSION
                };

                _rowItemSerializer.Serialize(newVersionItem, tempMemoryStream);
                foreach (var item in latestSnapshotCache.GetItems())
                {
                    _rowItemSerializer.Serialize(item, tempMemoryStream);
                }

                var writeBuffer = tempMemoryStream.ToArray();

                await _appendStorage.AtomicReplaceAsync(writeBuffer, ct);
            }
            //  Release tasks
            foreach (var source in sources)
            {
                source.SetResult();
            }
        }

        private async Task PersistBatchAsync(CancellationToken ct)
        {
            using (var bufferStream = new MemoryStream())
            {
                var sources = new List<TaskCompletionSource>();

                while (true)
                {
                    if (!_queue.TryPeek(out var queueItem)
                        || bufferStream.Length + queueItem.Buffer.Length
                            > _appendStorage.MaxBufferSize)
                    {
                        if (bufferStream.Length == 0)
                        {
                            throw new InvalidDataException("No buffer to append");
                        }
                        await _appendStorage.AtomicAppendAsync(bufferStream.ToArray(), ct);
                        //  Release tasks
                        foreach (var source in sources)
                        {
                            source.SetResult();
                        }

                        return;
                    }
                    else
                    {
                        if (!_queue.TryDequeue(out queueItem))
                        {
                            throw new InvalidOperationException(
                                "We dequeue what we just peeked, this shouldn't fail");
                        }
                        else
                        {
                            bufferStream.Write(queueItem.Buffer);
                            if (queueItem.TaskSource != null)
                            {
                                sources.Add(queueItem.TaskSource);
                            }
                        }
                    }
                }
            }
        }
    }
}