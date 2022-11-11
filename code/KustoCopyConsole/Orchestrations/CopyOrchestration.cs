using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Kusto.Data;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public class CopyOrchestration
    {
        private readonly MainParameterization _parameterization;

        public static async Task CopyAsync(MainParameterization parameterization)
        {
            var connectionMaker = ConnectionMaker.Create(parameterization);

            Console.WriteLine("Acquiring lock on folder...");
            await using (var blobLock = await CreateLockAsync(
                connectionMaker.LakeContainerClient,
                connectionMaker.LakeFolderClient.Path))
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

        private Task RunAsync()
        {
            throw new NotImplementedException();
        }
    }
}