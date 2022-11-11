using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Microsoft.Identity.Client.Platforms.Features.DesktopOs.Kerberos;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
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

                throw new NotImplementedException();
                //await PersistItemsAsync(checkpointGateway, new StatusItem[0], true, ct);

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
    }
}