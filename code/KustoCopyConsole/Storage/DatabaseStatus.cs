using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    public class DatabaseStatus
    {
        public static async Task<DatabaseStatus> RetrieveAsync(
            DataLakeDirectoryClient lakeFolderClient,
            BlobContainerClient lakeContainerClient,
            CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}