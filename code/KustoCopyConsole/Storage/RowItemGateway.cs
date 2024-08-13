using CsvHelper;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
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

        public static async Task<RowItemGateway> CreateAsync(
            IAppendStorage appendStorage,
            CancellationToken ct)
        {
            var readBuffer = await appendStorage.LoadAllAsync(ct);
            var allItems = readBuffer.Length == 0
                ? Array.Empty<RowItem>()
                : DeserializeBuffer(readBuffer);

            if (allItems.Any())
            {
                var versionItem = allItems.First();

                if (!Version.TryParse(versionItem.FileVersion, out var version)
                    || version != CURRENT_FILE_VERSION)
                {
                    throw new CopyException($"Incompatible file version:  '{version}'", false);
                }
                allItems = allItems.Skip(1);
            }

            var newVersionItem = new RowItem
            {
                FileVersion = CURRENT_FILE_VERSION.ToString(),
                RowType = RowType.FileVersion,
            };

            foreach (var item in allItems)
            {
                item.Validate();
            }

            var cache = new RowItemInMemoryCache(allItems);

            using (var tempMemoryStream = new MemoryStream())
            using (var writer = new StreamWriter(tempMemoryStream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteHeader<RowItem>();
                csv.NextRecord();
                csv.WriteRecords(cache.GetItems().Prepend(newVersionItem));
                csv.NextRecord();
                csv.Flush();
                writer.Flush();

                var writeBuffer = tempMemoryStream.ToArray();
                
                await appendStorage.AtomicReplaceAsync(writeBuffer, ct);

                return new RowItemGateway(appendStorage, cache);
            }
        }

        private static IEnumerable<RowItem> DeserializeBuffer(byte[] readBuffer)
        {
            using (var bufferStream = new MemoryStream(readBuffer))
            using (var reader = new StreamReader(bufferStream))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                var allItems = csv.GetRecords<RowItem>()
                    .ToImmutableArray();

                return allItems;
            }
        }
        #endregion

        public RowItemInMemoryCache InMemoryCache { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await FlushAsync(CancellationToken.None);
            await _appendStorage.DisposeAsync();
        }

        public async Task<RowItemAppend> AppendAsync(RowItem item, CancellationToken ct)
        {
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
                    if (_bufferStream.Length + binaryItem.Length <= _appendStorage.MaxBufferSize)
                    {
                        _bufferStream.Write(binaryItem);
                    }
                    else
                    {
                        QueueCurrentStream();
                    }
                    OnRowItemAppended(package);

                    return package;
                }
            }
            finally
            {
                await WriteQueueAsync(ct);
                await _backgroundTaskContainer.ObserveCompletedTasksAsync();
            }
        }

        public async Task FlushAsync(CancellationToken ct)
        {
            QueueCurrentStream();
            await WriteQueueAsync(ct);
            await _backgroundTaskContainer.ObserveCompletedTasksAsync();
        }

        private static byte[] GetBytes(RowItem item)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteRecord(item);
                csv.NextRecord();
                csv.Flush();
                writer.Flush();

                return stream.ToArray();
            }
        }

        private async Task WriteQueueAsync(CancellationToken ct)
        {
            using (var disposableLock = _asyncLock.TryGetLock())
            {
                if (disposableLock != null)
                {
                    while (_bufferToWriteQueue.TryDequeue(out var queueItem))
                    {
                        var success = await _appendStorage.AtomicAppendAsync(queueItem.Buffer, ct);

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
            }
            //  Combat racing condition
            if(_bufferToWriteQueue.Any())
            {
                await WriteQueueAsync(ct);
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
                    _bufferToWriteQueue.Enqueue(new QueueItem(
                        _bufferStream.ToArray(),
                        _persistanceTaskSource));
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