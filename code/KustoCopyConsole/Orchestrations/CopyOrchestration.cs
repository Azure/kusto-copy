using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
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
        private readonly KustoExportQueue _sourceExportQueue;
        private readonly KustoIngestQueue _destinationIngestQueue;
        private readonly DataLakeDirectoryClient _lakeFolderClient;
        private readonly IImmutableList<DatabaseStatus> _dbStatusList;

        #region Bootstrap
        public static async Task CopyAsync(
            MainParameterization parameterization,
            CancellationToken ct)
        {
            var connectionFactory = ConnectionsFactory.Create(parameterization);

            Trace.WriteLine("Acquiring lock on folder...");
            await using (var blobLock = await CreateLockAsync(
                connectionFactory.LakeContainerClient,
                connectionFactory.LakeFolderClient.Path))
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
                                d.Name!,
                                connectionFactory.LakeFolderClient,
                                connectionFactory.LakeContainerClient,
                                ct))
                            .ToImmutableArray();

                        await Task.WhenAll(dbStatusTasks);

                        var dbStatusList = dbStatusTasks
                            .Select(t => t.Result)
                            .ToImmutableArray();
                        var orchestration = new CopyOrchestration(
                            parameterization,
                            connectionFactory.SourceExportQueue!,
                            connectionFactory.DestinationIngestQueue!,
                            connectionFactory.LakeFolderClient,
                            dbStatusList);

                        Trace.WriteLine("Copying orchestration started");
                        await orchestration.RunAsync(ct);
                        Trace.WriteLine("Copying orchestration completed");
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }
            }
        }

        private CopyOrchestration(
            MainParameterization parameterization,
            KustoExportQueue sourceExportQueue,
            KustoIngestQueue destinationIngestQueue,
            DataLakeDirectoryClient lakeFolderClient,
            IImmutableList<DatabaseStatus> dbStatusList)
        {
            _parameterization = parameterization;
            _sourceExportQueue = sourceExportQueue;
            _destinationIngestQueue = destinationIngestQueue;
            _lakeFolderClient = lakeFolderClient;
            _dbStatusList = dbStatusList;
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            var dbParameterizationIndex = _parameterization.Source!.Databases!
                .ToImmutableDictionary(d => d.Name!);
            var setupTasks = _dbStatusList
                .Select(dbStatus => DestinationDbSetupOrchestration.SetupAsync(
                    dbStatus,
                    _destinationIngestQueue.Client,
                    ct))
                .ToImmutableArray();
            var planningTasks = _dbStatusList
                .Select(dbStatus => DbPlanningOrchestration.PlanAsync(
                    _parameterization.IsContinuousRun,
                    dbParameterizationIndex[dbStatus.DbName],
                    dbStatus,
                    _sourceExportQueue.Client,
                    ct))
                .ToImmutableArray();
            var exportingTasks = _dbStatusList
                .Select(dbStatus => DbExportingOrchestration.ExportAsync(
                    _parameterization.IsContinuousRun,
                    Task.WhenAll(planningTasks),
                    dbStatus,
                    _sourceExportQueue,
                    ct))
                .ToImmutableArray();
            var stagingTasks = _dbStatusList
                .Select(dbStatus => DbStagingOrchestration.StageAsync(
                    _parameterization.IsContinuousRun,
                    Task.WhenAll(exportingTasks),
                    dbStatus,
                    _destinationIngestQueue,
                    ct))
                .ToImmutableArray();
            var movingTasks = _dbStatusList
                .Select(dbStatus => DbMovingOrchestration.StageAsync(
                    _parameterization.IsContinuousRun,
                    Task.WhenAll(stagingTasks),
                    dbStatus,
                    _destinationIngestQueue.Client,
                    ct))
                .ToImmutableArray();
            var completeTasks = _dbStatusList
                .Select(dbStatus => DbCompletingOrchestration.StageAsync(
                    _parameterization.IsContinuousRun,
                    Task.WhenAll(movingTasks),
                    dbStatus,
                    _destinationIngestQueue.Client,
                    ct))
                .ToImmutableArray();
            var allTasks = setupTasks
                .Concat(planningTasks)
                .Concat(exportingTasks)
                .Concat(stagingTasks)
                .Concat(movingTasks)
                .Concat(completeTasks);

            await Task.WhenAll(allTasks);
        }

        private static async Task<IAsyncDisposable?> CreateLockAsync(
            BlobContainerClient lakeContainerClient,
            string folderPath)
        {
            var lockBlob = lakeContainerClient.GetAppendBlobClient($"{folderPath}/lock");

            await lockBlob.CreateIfNotExistsAsync();

            return await BlobLock.CreateAsync(lockBlob);
        }
    }
}