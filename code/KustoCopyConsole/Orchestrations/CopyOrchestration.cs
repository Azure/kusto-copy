﻿using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Kusto.Data;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    internal class CopyOrchestration
    {
        private readonly MainParameterization _parameterization;

        private CopyOrchestration(MainParameterization parameterization)
        {
            _parameterization = parameterization;
        }

        internal static async Task CopyAsync(MainParameterization parameterization)
        {
            var connectionMaker = ConnectionMaker.Create(parameterization);

            Console.WriteLine("Acquiring lock on folder...");
            await using (var blobLock =
                await CreateLockAsync(connectionMaker.LakeFolderBlobClient))
            {
                if (blobLock == null)
                {
                    Console.WriteLine("Can't acquire lock on folder");
                }
                else
                {
                    var orchestration = new CopyOrchestration(parameterization);

                    await orchestration.RunAsync();
                }
            }
        }

        private static async Task<IAsyncDisposable?> CreateLockAsync(BlobClient lakeFolderBlobClient)
        {
            var containerClient = lakeFolderBlobClient.GetParentBlobContainerClient();
            var lockBlob = containerClient.GetAppendBlobClient($"{lakeFolderBlobClient.Name}/lock");

            await lockBlob.CreateIfNotExistsAsync();

            return await BlobLock.CreateAsync(lockBlob);
        }

        private Task RunAsync()
        {
            throw new NotImplementedException();
        }
    }
}