using CsvHelper;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
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

        private readonly IAppendStorage _appendStorage;
        private readonly Func<IEnumerable<RowItem>, IEnumerable<RowItem>> _compactFunc;
        private readonly MemoryStream _memoryStream = new MemoryStream();

        public RowItemGateway(
            IAppendStorage appendStorage,
            Func<IEnumerable<RowItem>, IEnumerable<RowItem>> compactFunc)
        {
            _appendStorage = appendStorage;
            _compactFunc = compactFunc;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await FlushAsync();
            await _appendStorage.DisposeAsync();
        }

        public async Task<IImmutableList<RowItem>> MigrateToLatestVersionAsync(CancellationToken ct)
        {
            return await CompactAsync(ct);
        }

        public Task AppendAsync(RowItem item, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        public Task AppendAtomicallyAsync(IEnumerable<RowItem> items, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        public async Task FlushAsync()
        {
            await Task.CompletedTask;
        }

        private async Task<IImmutableList<RowItem>> CompactAsync(CancellationToken ct)
        {
            var readBuffer = await _appendStorage.LoadAllAsync(ct);

            if (readBuffer.Length == 0)
            {
                var versionEntity = new VersionEntity(CURRENT_FILE_VERSION) as IRowItemSerializable;
                var versionItem = versionEntity.Serialize();

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
                    csv.NextRecord();
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

                return allNewItems.ToImmutableArray();
            }
        }
    }
}