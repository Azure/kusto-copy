using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Kusto.Data;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;

namespace KustoCopyConsole.Orchestrations
{
    public class CopyOrchestration
    {
        private readonly MainParameterization _parameterization;

        public static async Task CopyAsync(
            MainParameterization parameterization,
            CancellationToken ct)
        {
            var connectionMaker = ConnectionMaker.Create(parameterization);

            Trace.WriteLine("Acquiring lock on folder...");
            await using (var blobLock = await CreateLockAsync(
                connectionMaker.LakeContainerClient,
                connectionMaker.LakeFolderClient.Path))
            {
                if (blobLock == null)
                {
                    Trace.WriteLine("Can't acquire lock on folder");
                }
                else
                {
                    if (parameterization.Source != null)
                    {
                        var dbStatusTasks = parameterization.Source.Databases
                            .Select(d => DatabaseStatus.RetrieveAsync(
                                connectionMaker.LakeFolderClient,
                                connectionMaker.LakeContainerClient,
                                ct))
                            .ToImmutableArray();

                        await Task.WhenAll(dbStatusTasks);
                    }

                    var orchestration = new CopyOrchestration(parameterization);

                    await orchestration.RunAsync(ct);
                }
            }
        }

        private CopyOrchestration(MainParameterization parameterization)
        {
            _parameterization = parameterization;
        }

        private static async Task<IAsyncDisposable?> CreateLockAsync(
            BlobContainerClient lakeContainerClient,
            string folderPath)
        {
            var lockBlob = lakeContainerClient.GetAppendBlobClient($"{folderPath}/lock");

            await lockBlob.CreateIfNotExistsAsync();

            return await BlobLock.CreateAsync(lockBlob);
        }

        private Task RunAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}