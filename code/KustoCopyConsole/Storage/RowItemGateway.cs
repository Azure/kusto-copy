using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal class RowItemGateway : IAsyncDisposable
    {
        #region Inner Types
        private record QueueItem(byte[] Buffer, TaskCompletionSource TaskSource);

        private record PersistanceAsyncLock(
            TaskCompletionSource EnterSource,
            TaskCompletionSource ExitSource);

        private record AppendTrial(
            IEnumerable<Task> TasksToAwait,
            RowItemAppend? RowItemAppend,
            PersistanceAsyncLock? PersistanceAsyncLock,
            Task? PersistanceTask,
            byte[]? BufferToPersist);
        #endregion

        private static readonly Version CURRENT_FILE_VERSION = new Version(0, 0, 1, 0);
        private static readonly TimeSpan FLUSH_PERIOD = TimeSpan.FromSeconds(5);

        private static readonly RowItemSerializer _rowItemSerializer = CreateRowItemSerializer();

        private readonly IAppendStorage _appendStorage;
        //  The lock object is used to lock access to the stream and switch around the task source
        private readonly object _lock = new object();
        private readonly MemoryStream _bufferStream = new();
        private readonly ConcurrentQueue<Task> _schedulePersistanceQueue = new();
        private volatile PersistanceAsyncLock _persistanceAsyncLock = new(
            new TaskCompletionSource(),
            new TaskCompletionSource());

        public event EventHandler<RowItemAppend>? RowItemAppended;

        #region Construction
        private RowItemGateway(IAppendStorage appendStorage, RowItemInMemoryCache cache)
        {
            _appendStorage = appendStorage;
            InMemoryCache = cache;
        }

        private static RowItemSerializer CreateRowItemSerializer()
        {
            return new RowItemSerializer()
                .AddType<FileVersionRowItem>(RowType.FileVersion)
                .AddType<IterationRowItem>(RowType.SourceTable)
                .AddType<BlockRowItem>(RowType.SourceBlock)
                .AddType<UrlRowItem>(RowType.SourceUrl);
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

                return new RowItemGateway(appendStorage, cache);
            }
        }
        #endregion

        public RowItemInMemoryCache InMemoryCache { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await FlushAsync(CancellationToken.None);
            await _appendStorage.DisposeAsync();
        }

        public async Task<RowItemAppend> AppendAsync(RowItemBase item, CancellationToken ct)
        {
            item.Validate();

            var binaryItem = GetBytes(item);

            return await AppendInternalAsync(item, binaryItem, ct);
        }

        public async Task FlushAsync(CancellationToken ct)
        {
            while (_schedulePersistanceQueue.TryDequeue(out var task))
            {
                await task;
            }
        }

        private async Task<RowItemAppend> AppendInternalAsync(
            RowItemBase item,
            byte[] binaryItem,
            CancellationToken ct)
        {
            while (true)
            {
                var appendTrial = TryAppendInBuffer(item, binaryItem);

                await Task.WhenAll(appendTrial.TasksToAwait);
                if (appendTrial.RowItemAppend != null)
                {
                    Func<Task> schedulePersistance = async () =>
                    {
                        await Task.Delay(FLUSH_PERIOD, ct);

                        var buffer = TryAcquirePersistanceAsyncLock();

                        if (buffer != null)
                        {
                            await PersistBufferAsync(buffer, ct);
                        }
                    };

                    _schedulePersistanceQueue.Enqueue(schedulePersistance());

                    return appendTrial.RowItemAppend;
                }
                else if (appendTrial.BufferToPersist != null)
                {
                    await PersistBufferAsync(appendTrial.BufferToPersist, ct);
                }
                else if (appendTrial.PersistanceTask != null)
                {   //  Wait for the thread doing the work
                    await appendTrial.PersistanceTask;
                }
                else
                {
                    throw new InvalidOperationException("Shouldn't get here");
                }
            }
        }

        private async Task PersistBufferAsync(byte[] buffer, CancellationToken ct)
        {
            var oldPersistanceAsyncLock = _persistanceAsyncLock;

            await _appendStorage.AtomicAppendAsync(buffer, ct);
            Interlocked.Exchange(ref _persistanceAsyncLock, new PersistanceAsyncLock(
                new TaskCompletionSource(),
                new TaskCompletionSource()));
            //  Unblock waiting threads
            oldPersistanceAsyncLock.ExitSource.SetResult();
        }

        private AppendTrial TryAppendInBuffer(RowItemBase item, byte[] binaryItem)
        {
            lock (_lock)
            {
                var tasks = DequeueTasksToAwait();

                if (_bufferStream.Length + binaryItem.Length <= _appendStorage.MaxBufferSize)
                {
                    var package = new RowItemAppend(item, _persistanceAsyncLock.ExitSource.Task);

                    _bufferStream.Write(binaryItem);
                    OnRowItemAppended(package);

                    return new AppendTrial(tasks, package, _persistanceAsyncLock, null, null);
                }
                else
                {
                    var buffer = TryAcquirePersistanceAsyncLock();

                    if (buffer != null)
                    {
                        return new AppendTrial(tasks, null, null, null, buffer);
                    }
                    else
                    {
                        return new AppendTrial(
                            tasks, null, null, _persistanceAsyncLock.ExitSource.Task, null);
                    }
                }
            }
        }

        private IEnumerable<Task> DequeueTasksToAwait()
        {
            lock (_lock)
            {
                var tasks = new List<Task>();

                while (_schedulePersistanceQueue.TryPeek(out var task) && task.IsCompleted)
                {
                    if (_schedulePersistanceQueue.TryDequeue(out var dequeuedTask))
                    {
                        tasks.Add(task);
                    }
                }

                return tasks;
            }
        }

        private byte[]? TryAcquirePersistanceAsyncLock()
        {
            lock (_lock)
            {
                if (_persistanceAsyncLock.EnterSource.TrySetResult())
                {
                    var buffer = _bufferStream.ToArray();

                    _bufferStream.SetLength(0);

                    return buffer;
                }
                else
                {
                    return null;
                }
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

        private void OnRowItemAppended(RowItemAppend package)
        {
            InMemoryCache.AppendItem(package.Item);
            if (RowItemAppended != null)
            {
                RowItemAppended(this, package);
            }
        }
    }
}