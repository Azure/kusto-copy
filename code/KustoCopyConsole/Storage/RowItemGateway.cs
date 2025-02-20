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
        private record QueuedRowItem(
            DateTime EnqueueTime,
            byte[] Buffer,
            RowItemInMemoryCache? SnapshotCache,
            TaskCompletionSource? TaskSource);
        #endregion

        private const long MAX_VOLUME_BEFORE_SNAPSHOT = 20000000;
        private static readonly TimeSpan MIN_WAIT_PERIOD = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan FLUSH_PERIOD = TimeSpan.FromSeconds(5);

        private static readonly RowItemSerializer _rowItemSerializer = CreateRowItemSerializer();

        private readonly LogStorage _logStorage;
        private readonly ConcurrentQueue<QueuedRowItem> _rowItemQueue = new();
        private readonly ConcurrentQueue<Task> _releaseSourceTaskQueue = new();
        private readonly Task _backgroundTask;
        private readonly TaskCompletionSource _backgroundCompletedSource = new();
        private readonly object _lock = new object();
        private long _volumeSinceLastSnapshot = 0;
        private volatile RowItemInMemoryCache _inMemoryCache;

        #region Construction
        private RowItemGateway(
            LogStorage logStorage,
            RowItemInMemoryCache cache,
            CancellationToken ct)
        {
            _logStorage = logStorage;
            _backgroundTask = Task.Run(() => BackgroundPersistanceAsync(ct));
            _inMemoryCache = cache;
        }

        public static async Task<RowItemGateway> CreateAsync(
            LogStorage logStorage,
            CancellationToken ct)
        {
            var cache = new RowItemInMemoryCache();

            await foreach (var chunk in logStorage.ReadLatestViewAsync(ct))
            {
                using (var stream = chunk.Stream)
                using (var reader = new StreamReader(stream))
                {
                    string? line;

                    while ((line = await reader.ReadLineAsync()) != null)
                    {
                        var item = _rowItemSerializer.Deserialize(line);

                        cache = cache.AppendItem(item);
                    }
                }
            }
            await logStorage.WriteLatestViewAsync(StreamCache(cache), ct);

            return new RowItemGateway(logStorage, cache, ct);
        }

        private static RowItemSerializer CreateRowItemSerializer()
        {
            return new RowItemSerializer()
                .AddType<ActivityRowItem>(RowType.Activity)
                .AddType<IterationRowItem>(RowType.Iteration)
                .AddType<TempTableRowItem>(RowType.TempTable)
                .AddType<BlockRowItem>(RowType.Block)
                .AddType<UrlRowItem>(RowType.Url)
                .AddType<ExtentRowItem>(RowType.Extent);
        }

        private static IEnumerable<byte> StreamCache(RowItemInMemoryCache cache)
        {
            foreach (var item in cache.GetItems())
            {
                var text = _rowItemSerializer.Serialize(item);
                var buffer = ASCIIEncoding.UTF8.GetBytes(text);

                foreach (var character in buffer)
                {
                    yield return character;
                }
            }
        }
        #endregion

        public event EventHandler<RowItemBase>? RowItemAppended;

        public RowItemInMemoryCache InMemoryCache => _inMemoryCache;

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _backgroundCompletedSource.SetResult();
            await _backgroundTask;
            await Task.WhenAll(_releaseSourceTaskQueue);
        }

        public void Append(RowItemBase item)
        {
            AppendInternal(new[] { item }, null);
        }

        public void Append(IEnumerable<RowItemBase> items)
        {
            AppendInternal(items, null);
        }

        public Task AppendAndPersistAsync(RowItemBase item, CancellationToken ct)
        {
            var taskSource = new TaskCompletionSource();

            AppendInternal(new[] { item }, taskSource);

            return taskSource.Task;
        }

        public async Task AppendAndPersistAsync(
            IEnumerable<RowItemBase> items,
            CancellationToken ct)
        {
            var materializedItems = items.ToImmutableArray();

            if (materializedItems.Any())
            {
                var taskSource = new TaskCompletionSource();

                AppendInternal(items, taskSource);
                await taskSource.Task;
            }
        }

        private void AppendInternal(
            IEnumerable<RowItemBase> items,
            TaskCompletionSource? TaskSource)
        {
            var materializedItems = items.ToImmutableArray();
            var binaryItems = new List<byte[]>();
            RowItemInMemoryCache? snapshot = null;

            foreach (var item in materializedItems)
            {
                item.Validate();

                var text = _rowItemSerializer.Serialize(item);
                var binaryItem = ASCIIEncoding.ASCII.GetBytes(text);

                binaryItems.Add(binaryItem);
            }
            lock (_lock)
            {
                var newCache = _inMemoryCache;

                foreach (var item in materializedItems)
                {
                    newCache = newCache.AppendItem(item);
                }
                Interlocked.Exchange(ref _inMemoryCache, newCache);
                Interlocked.Add(ref _volumeSinceLastSnapshot, binaryItems.Sum(i => i.Length));
                if (_volumeSinceLastSnapshot > MAX_VOLUME_BEFORE_SNAPSHOT)
                {
                    snapshot = newCache;
                    Interlocked.Exchange(ref _volumeSinceLastSnapshot, 0);
                }
            }
            foreach (var binaryItem in binaryItems)
            {
                _rowItemQueue.Enqueue(new QueuedRowItem(DateTime.Now, binaryItem, snapshot, TaskSource));
            }
            foreach (var item in materializedItems)
            {
                OnRowItemAppended(item);
            }
        }

        private void OnRowItemAppended(RowItemBase item)
        {
            if (RowItemAppended != null)
            {
                RowItemAppended(this, item);
            }
        }

        private async Task BackgroundPersistanceAsync(CancellationToken ct)
        {
            while (!_backgroundCompletedSource.Task.IsCompleted)
            {
                if (_rowItemQueue.TryPeek(out var queueItem))
                {
                    var delta = DateTime.Now - queueItem.EnqueueTime;
                    var waitTime = FLUSH_PERIOD - delta;

                    if (waitTime < MIN_WAIT_PERIOD)
                    {
                        await PersistBatchAsync(ct);
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
                await CleanReleaseSourceTaskQueueAsync();
            }
        }

        private async Task CleanReleaseSourceTaskQueueAsync()
        {
            while (_releaseSourceTaskQueue.TryPeek(out var task) && task.IsCompleted)
            {
                if (_releaseSourceTaskQueue.TryDequeue(out var task2))
                {
                    await task2;
                }
            }
        }

        private async Task PersistBatchAsync(CancellationToken ct)
        {
            using (var bufferStream = new MemoryStream())
            {
                var sources = new List<TaskCompletionSource>();
                RowItemInMemoryCache? lastCache = null;

                while (true)
                {
                    if (!_rowItemQueue.TryPeek(out var queueItem)
                        || bufferStream.Length + queueItem.Buffer.Length
                            > _logStorage.MaxBufferSize)
                    {   //  Flush buffer stream
                        if (bufferStream.Length == 0)
                        {
                            throw new InvalidDataException("No buffer to append");
                        }
                        await _logStorage.AtomicAppendAsync(bufferStream.ToArray(), ct);
                        //  Release tasks
                        foreach (var source in sources)
                        {   //  We run it on another thread not to block the persistance
                            _releaseSourceTaskQueue.Enqueue(Task.Run(() => source.TrySetResult()));
                        }
                        if (lastCache != null)
                        {
                            await _logStorage.WriteLatestViewAsync(StreamCache(lastCache), ct);
                        }

                        return;
                    }
                    else
                    {   //  Append to buffer stream
                        if (_rowItemQueue.TryDequeue(out queueItem))
                        {
                            lastCache = queueItem.SnapshotCache ?? lastCache;
                            bufferStream.Write(queueItem.Buffer);
                            if (queueItem.TaskSource != null)
                            {
                                sources.Add(queueItem.TaskSource);
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException(
                                "We dequeue what we just peeked, this shouldn't fail");
                        }
                    }
                }
            }
        }
    }
}