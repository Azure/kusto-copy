﻿using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using CsvHelper;
using Microsoft.Identity.Client.Platforms.Features.DesktopOs.Kerberos;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    public class DatabaseStatus
    {
        #region Inner Types
        private class StatusItemIndex
        {
            private readonly List<StatusItem> _iterationItems;

            public StatusItemIndex(IEnumerable<StatusItem> items)
            {
                _iterationItems = items
                    .Where(i => i.Level == HierarchyLevel.Iteration)
                    .OrderBy(i => i.IterationId)
                    .ToList();
            }

            public IEnumerable<StatusItem> AllItems => _iterationItems;

            public IImmutableList<StatusItem> Iterations => _iterationItems.ToImmutableArray();
        }
        #endregion

        private readonly StatusItemIndex _statusIndex;
        private CheckpointGateway _checkpointGateway;

        #region Constructors
        public static async Task<DatabaseStatus> RetrieveAsync(
            string dbName,
            DataLakeDirectoryClient lakeFolderClient,
            BlobContainerClient lakeContainerClient,
            CancellationToken ct)
        {
            var checkpointGateway = new CheckpointGateway(
                lakeFolderClient.GetSubDirectoryClient($"db.{dbName}"),
                lakeContainerClient);

            if (!(await checkpointGateway.ExistsAsync(ct)))
            {
                await checkpointGateway.CreateAsync(ct);

                await PersistItemsAsync(checkpointGateway, new StatusItem[0], true, ct);

                return new DatabaseStatus(
                    dbName,
                    checkpointGateway,
                    new StatusItem[0]);
            }
            else
            {
                var buffer = await checkpointGateway.ReadAllContentAsync(ct);

                //  Rewrite the content in one clean append-blob
                //  Ensure it's an append blob + compact it
                Trace.TraceInformation("Rewrite checkpoint blob...");

                var items = ParseCsv(buffer);
                var databaseStatus = new DatabaseStatus(dbName, checkpointGateway, items);

                await databaseStatus.CompactAsync(ct);

                return databaseStatus;
            }
        }

        private DatabaseStatus(
            string dbName,
            CheckpointGateway checkpointGateway,
            IEnumerable<StatusItem> statusItems)
        {
            var latestItems = statusItems
                .GroupBy(i => i.IterationId)
                .Select(g => g.MaxBy(i => i.Timestamp)!);

            DbName = dbName;
            _statusIndex = new StatusItemIndex(latestItems);
            _checkpointGateway = checkpointGateway;
        }
        #endregion

        public string DbName { get; }

        public Uri IndexBlobUri => _checkpointGateway.BlobUri;

        public IImmutableList<StatusItem> GetIterations()
        {
            return _statusIndex.Iterations;
        }

        public async Task PersistNewItemsAsync(
            IEnumerable<StatusItem> items,
            CancellationToken ct)
        {
            if (items.Count() == 0)
            {
                throw new ArgumentException("Is empty", nameof(items));
            }

            if (_checkpointGateway.CanWrite)
            {
                await PersistItemsAsync(_checkpointGateway, items, false, ct);
            }
            else
            {
                await CompactAsync(ct);
                await PersistNewItemsAsync(items, ct);
            }
        }

        private static IImmutableList<StatusItem> ParseCsv(byte[] buffer)
        {
            const bool VALIDATE_HEADER = true;

            if (!buffer.Any())
            {
                return ImmutableArray<StatusItem>.Empty;
            }
            else
            {
                using (var stream = new MemoryStream(buffer))
                using (var reader = new StreamReader(stream))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    if (VALIDATE_HEADER)
                    {
                        csv.Read();
                        csv.ReadHeader();
                        csv.ValidateHeader<StatusItem>();
                    }
                    var items = csv.GetRecords<StatusItem>();

                    return items.ToImmutableArray();
                }
            }
        }

        private static async Task PersistItemsAsync(
            CheckpointGateway checkpointGateway,
            IEnumerable<StatusItem> items,
            bool persistHeaders,
            CancellationToken ct)
        {
            const long MAX_LENGTH = 4000000;

            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                if (persistHeaders)
                {
                    csv.WriteHeader<StatusItem>();
                    csv.NextRecord();
                }

                foreach (var item in items)
                {
                    var positionBefore = stream.Position;

                    csv.WriteRecord(item);
                    csv.NextRecord();
                    csv.Flush();
                    writer.Flush();

                    var positionAfter = stream.Position;

                    if (positionAfter > MAX_LENGTH)
                    {
                        stream.SetLength(positionBefore);
                        stream.Flush();
                        await checkpointGateway.WriteAsync(stream.ToArray(), ct);
                        stream.SetLength(0);
                        csv.WriteRecord(item);
                        csv.NextRecord();
                    }
                }
                csv.Flush();
                writer.Flush();
                if (stream.Position > 0)
                {
                    stream.Flush();
                    await checkpointGateway.WriteAsync(stream.ToArray(), ct);
                }
            }
        }

        private async Task CompactAsync(CancellationToken ct)
        {
            var tempCheckpointGateway =
                await _checkpointGateway.GetTemporaryCheckpointGatewayAsync(ct);

            await PersistItemsAsync(tempCheckpointGateway, _statusIndex.AllItems, true, ct);
            _checkpointGateway = await tempCheckpointGateway.MoveOutOfTemporaryAsync(ct);
        }
    }
}