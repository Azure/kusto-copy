using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using CsvHelper;
using Microsoft.Identity.Client.Platforms.Features.DesktopOs.Kerberos;
using System;
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
        public static async Task<DatabaseStatus> RetrieveAsync(
            string? dbName,
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
                throw new NotImplementedException();

                //return new DatabaseStatus(
                //    checkpointGateway,
                //    tableNames,
                //    new StatusItem[0]);
            }
            else
            {
                var buffer = await checkpointGateway.ReadAllContentAsync(ct);

                //  Rewrite the content in one clean append-blob
                //  Ensure it's an append blob + compact it
                Trace.TraceInformation("Rewrite checkpoint blob...");

                //var items = ParseCsv(buffer);
                //var globalTableStatus =
                //    new GlobalTableStatus(checkpointGateway, tableNames, items);

                //await globalTableStatus.CompactAsync(ct);

                //return globalTableStatus;
                throw new NotImplementedException();
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
    }
}