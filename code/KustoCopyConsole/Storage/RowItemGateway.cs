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
            DateTime EnqueueTime,
            byte[] Buffer,
            RowItemInMemoryCache? SnapshotCache,
            TaskCompletionSource? TaskSource);
        #endregion

        private static readonly TimeSpan MIN_WAIT_PERIOD = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan FLUSH_PERIOD = TimeSpan.FromSeconds(5);

        private static readonly RowItemSerializer _rowItemSerializer = CreateRowItemSerializer();

        private readonly LogStorage _logStorage;
        private readonly ConcurrentQueue<QueueItem> _queue = new();
        private readonly Task _backgroundTask;
        private readonly TaskCompletionSource _backgroundCompletedSource = new();
        private readonly object _lock = new object();
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

        private static RowItemSerializer CreateRowItemSerializer()
        {
            return new RowItemSerializer()
                .AddType<FileVersionRowItem>(RowType.FileVersion)
                .AddType<ActivityRowItem>(RowType.Activity)
                .AddType<IterationRowItem>(RowType.Iteration)
                .AddType<TempTableRowItem>(RowType.TempTable)
                .AddType<BlockRowItem>(RowType.Block)
                .AddType<UrlRowItem>(RowType.Url)
                .AddType<ExtentRowItem>(RowType.Extent);
        }

        public static async Task<RowItemGateway> CreateAsync(
            LogStorage logStorage,
            Version appVersion,
            CancellationToken ct)
        {
            var cache = new RowItemInMemoryCache();

            await foreach (var chunk in logStorage.ReadLatestViewAsync(ct))
            {
                using (var stream = chunk.Stream)
                using (var reader = new StreamReader(stream))
                {
                    var firstLine = await reader.ReadLineAsync();

                    if (firstLine != null)
                    {
                        var versionItem =
                            _rowItemSerializer.Deserialize(firstLine) as FileVersionRowItem;

                        if (versionItem != null)
                        {
                            //  Eventually validate version
                            string? line;

                            while ((line = await reader.ReadLineAsync()) != null)
                            {
                                var item = _rowItemSerializer.Deserialize(firstLine);

                                cache = cache.AppendItem(item);
                            }
                        }
                        else
                        {
                            throw new InvalidDataException(
                                $"Expect the first row to be a version row:  {firstLine}");
                        }
                    }
                }
            }
            await logStorage.WriteLatestViewAsync(StreamCache(cache, appVersion), ct);

            return new RowItemGateway(logStorage, cache, ct);
        }

        private static IEnumerable<byte> StreamCache(
            RowItemInMemoryCache cache,
            Version appVersion)
        {
            var versionItem = new FileVersionRowItem { FileVersion = appVersion };
            var items = cache.GetItems().Prepend(versionItem);

            foreach (var item in items)
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

            var text = _rowItemSerializer.Serialize(item);
            var binaryItem = ASCIIEncoding.ASCII.GetBytes(text);

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

        private async Task BackgroundPersistanceAsync(CancellationToken ct)
        {
            while (!_backgroundCompletedSource.Task.IsCompleted)
            {
                if (_queue.TryPeek(out var queueItem))
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
                            > _logStorage.MaxBufferSize)
                    {
                        if (bufferStream.Length == 0)
                        {
                            throw new InvalidDataException("No buffer to append");
                        }
                        await _logStorage.AtomicAppendAsync(bufferStream.ToArray(), ct);
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