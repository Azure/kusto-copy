using CsvHelper;
using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Kusto.Cloud.Platform.Utils.CachedBufferEncoder;

namespace KustoCopyConsole.Storage
{
    internal class RowItemGateway : IAsyncDisposable
    {
        private static readonly Version CURRENT_FILE_VERSION = new Version(0, 0, 1, 0);
        private static readonly TimeSpan MAX_BUFFER_TIME = TimeSpan.FromSeconds(5);

        private readonly IAppendStorage _appendStorage;
        private readonly Func<IEnumerable<RowItem>, IEnumerable<RowItem>> _compactFunc;
        private readonly MemoryStream _bufferStream = new MemoryStream();
        private DateTime? _bufferStartTime;

        public event EventHandler<RowItem>? RowAppended;

        public RowItemGateway(
            IAppendStorage appendStorage,
            Func<IEnumerable<RowItem>, IEnumerable<RowItem>> compactFunc)
        {
            _appendStorage = appendStorage;
            _compactFunc = compactFunc;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await FlushAsync(CancellationToken.None);
            await _appendStorage.DisposeAsync();
        }

        public async Task<IImmutableList<RowItem>> MigrateToLatestVersionAsync(CancellationToken ct)
        {
            return await CompactAsync(ct);
        }

        public async Task AppendAsync(RowItem item, CancellationToken ct)
        {
            var bufferToWrite = AppendToBuffer(item);

            if (bufferToWrite == null
                && _bufferStartTime != null
                && DateTime.Now - _bufferStartTime > MAX_BUFFER_TIME)
            {
                await FlushAsync(ct);
                bufferToWrite = _bufferStream.ToArray();
                _bufferStream.SetLength(0);
                _bufferStartTime = null;
            }
            if (bufferToWrite != null)
            {
                await _appendStorage.AtomicAppendAsync(bufferToWrite, ct);
            }
        }

        public async Task FlushAsync(CancellationToken ct)
        {
            var bufferToWrite = _bufferStream.ToArray();

            _bufferStream.SetLength(0);
            _bufferStartTime = null;
            await _appendStorage.AtomicAppendAsync(bufferToWrite, ct);
        }

        #region Compaction
        private async Task<IImmutableList<RowItem>> CompactAsync(CancellationToken ct)
        {
            var readBuffer = await _appendStorage.LoadAllAsync(ct);

            if (readBuffer.Length == 0)
            {
                var versionItem = new RowItem
                {
                    FileVersion = CURRENT_FILE_VERSION.ToString(),
                    RowType = RowType.FileVersion,
                };

                versionItem.Validate();
                using (var tempMemoryStream = new MemoryStream())
                using (var writer = new StreamWriter(tempMemoryStream))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteHeader<RowItem>();
                    csv.NextRecord();
                    csv.WriteRecord(versionItem);
                    csv.NextRecord();
                    csv.Flush();
                    writer.Flush();

                    var writeBuffer = tempMemoryStream.ToArray();
                    var isValidWrite = await _appendStorage.AtomicAppendAsync(writeBuffer, ct);

                    if (!isValidWrite)
                    {
                        throw new CopyException("Initial log write fails", false);
                    }

                    return ImmutableList.Create(versionItem);
                }
            }
            else
            {
                var items = CompactBuffer(readBuffer);

                using (var tempMemoryStream = new MemoryStream())
                using (var writer = new StreamWriter(tempMemoryStream))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteHeader<RowItem>();
                    csv.NextRecord();
                    csv.WriteRecords(items);
                    csv.Flush();
                    writer.Flush();

                    var writeBuffer = tempMemoryStream.ToArray();

                    await _appendStorage.AtomicReplaceAsync(writeBuffer, ct);

                    return items;
                }
            }
        }

        private IImmutableList<RowItem> CompactBuffer(byte[] readBuffer)
        {
            using (var bufferStream = new MemoryStream(readBuffer))
            using (var reader = new StreamReader(bufferStream))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {   //  Skip header line
                if (!csv.Read())
                {
                    throw new CopyException("Can't read log header", false);
                }
                var versionItem = csv.GetRecord<RowItem>();

                if (!Version.TryParse(versionItem.FileVersion, out var version)
                    || version != CURRENT_FILE_VERSION)
                {
                    throw new CopyException($"Incompatible file version:  '{version}'", false);
                }

                var otherItems = csv.GetRecords<RowItem>();
                var compactedItems = _compactFunc(otherItems);
                var allNewItems = compactedItems.Prepend(versionItem);

                foreach (var item in allNewItems)
                {
                    item.Validate();
                }

                return allNewItems.ToImmutableArray();
            }
        }
        #endregion

        private byte[]? AppendToBuffer(RowItem item)
        {
            item.Validate();
            lock (_bufferStream)
            {
                var lengthBefore = _bufferStream.Length;

                using (var writer = new StreamWriter(_bufferStream, leaveOpen: true))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecord(item);
                    csv.NextRecord();
                    csv.Flush();
                    writer.Flush();
                }

                var lengthAfter = _bufferStream.Length;

                if (lengthAfter > _appendStorage.MaxBufferSize)
                {   //  Buffer is too long:  write buffer before this item
                    if (lengthBefore == 0)
                    {
                        throw new CopyException(
                            $"Buffer to write to the log is too long:  {lengthAfter}",
                            false);
                    }
                    _bufferStream.SetLength(lengthBefore);

                    var allBuffer = _bufferStream.ToArray();
                    var beforeBuffer = new byte[lengthBefore];
                    var remainBuffer = new byte[lengthAfter - lengthBefore];

                    Array.Copy(allBuffer, beforeBuffer, lengthBefore);
                    Array.Copy(allBuffer, lengthBefore, remainBuffer, 0, remainBuffer.Length);
                    _bufferStream.SetLength(0);
                    _bufferStream.Write(
                        allBuffer,
                        (int)lengthBefore,
                        (int)(lengthAfter - lengthBefore));
                    _bufferStartTime = DateTime.Now;

                    return beforeBuffer;
                }
                else if (_bufferStartTime == null)
                {
                    _bufferStartTime = DateTime.Now;
                }
            }

            return null;
        }
    }
}