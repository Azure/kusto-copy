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
        #endregion

        private static readonly Version CURRENT_FILE_VERSION = new Version(0, 0, 1, 0);
        private static readonly TimeSpan FLUSH_TIME = TimeSpan.FromSeconds(5);

        private static readonly RowItemSerializer _rowItemSerializer = CreateRowItemSerializer();

        private readonly IAppendStorage _appendStorage;
        private readonly BackgroundTaskContainer _backgroundTaskContainer = new();
        private readonly ConcurrentQueue<QueueItem> _bufferToWriteQueue = new();
        //  The lock object is used to lock access to the stream and switch around the task source
        private readonly object _lock = new object();
        private readonly MemoryStream _bufferStream = new();
        //  The async lock object is used to lock access to the storage and queued items
        //  in order to serialize access
        private readonly AsyncLock _asyncLock = new();
        private TaskCompletionSource _persistanceTaskSource = new();

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
                .AddType<SourceTableRowItem>(RowType.SourceTable)
                .AddType<SourceBlockRowItem>(RowType.SourceBlock)
                .AddType<SourceUrlRowItem>(RowType.SourceUrl)
                .AddType<DestinationTableRowItem>(RowType.DestinationTable)
                .AddType<DestinationBlockRowItem>(RowType.DestinationBlock);
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

            try
            {
                lock (_lock)
                {
                    var package = new RowItemAppend(item, _persistanceTaskSource.Task);

                    if (_bufferStream.Length == 0)
                    {
                        _backgroundTaskContainer.AddTask(
                            AutoPersistAsync(_persistanceTaskSource, ct));
                    }
                    if (_bufferStream.Length + binaryItem.Length > _appendStorage.MaxBufferSize)
                    {
                        QueueCurrentStream();
                    }
                    _bufferStream.Write(binaryItem);
                    OnRowItemAppended(package);

                    return package;
                }
            }
            finally
            {
                await WriteQueueAsync(ct);
                await _backgroundTaskContainer.ObserveCompletedTasksAsync(ct);
            }
        }

        public async Task FlushAsync(CancellationToken ct)
        {
            QueueCurrentStream();
            await WriteQueueAsync(ct);
            await _backgroundTaskContainer.ObserveCompletedTasksAsync(ct);
        }

        private static byte[] GetBytes(RowItemBase item)
        {
            using (var stream = new MemoryStream())
            {
                _rowItemSerializer.Serialize(item, stream);

                return stream.ToArray();
            }
        }

        private async Task WriteQueueAsync(CancellationToken ct)
        {
            //  Combat racing condition
            if (_bufferToWriteQueue.Any())
            {
                using (var disposableLock = _asyncLock.TryGetLock())
                {
                    if (disposableLock != null)
                    {
                        while (_bufferToWriteQueue.TryDequeue(out var queueItem))
                        {
                            var success =
                                await _appendStorage.AtomicAppendAsync(queueItem.Buffer, ct);

                            if (!success)
                            {
                                throw new NotImplementedException("Must compact");
                            }
                            else
                            {
                                queueItem.TaskSource.SetResult();
                            }
                        }
                    }
                    else
                    {   //  Another thread got the lock
                        return;
                    }
                }
            }
        }

        private async Task AutoPersistAsync(
            TaskCompletionSource persistanceTaskSource,
            CancellationToken ct)
        {   //  First wait for the flush time
            await Task.Delay(FLUSH_TIME);

            lock (_lock)
            {
                if (object.ReferenceEquals(persistanceTaskSource, _persistanceTaskSource))
                {
                    QueueCurrentStream();
                }
                else
                {
                    return;
                }
            }
            await WriteQueueAsync(ct);
        }

        private void QueueCurrentStream()
        {
            lock (_lock)
            {
                if (_bufferStream.Length > 0)
                {
                    var buffer = _bufferStream.ToArray();

                    if (buffer.Length > _appendStorage.MaxBufferSize)
                    {
                        throw new InvalidDataException(
                            $"Buffer stream is larger than accepted storage block:  " +
                            $"{buffer.Length}");
                    }
                    _bufferToWriteQueue.Enqueue(new QueueItem(buffer, _persistanceTaskSource));
                    _persistanceTaskSource = new();
                    _bufferStream.SetLength(0);
                }
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