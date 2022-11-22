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
        private readonly KustoQueuedClient _destinationClient;
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
                            connectionFactory.DestinationQueuedClient!,
                            connectionFactory.LakeFolderClient,
                            dbStatusList);

                        await orchestration.RunAsync(ct);
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
            KustoQueuedClient destinationClient,
            DataLakeDirectoryClient lakeFolderClient,
            IImmutableList<DatabaseStatus> dbStatusList)
        {
            _parameterization = parameterization;
            _sourceExportQueue = sourceExportQueue;
            _destinationClient = destinationClient;
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
                    _destinationClient,
                    ct))
                .ToImmutableArray();
            var planningTasks = _dbStatusList
                .Select(dbStatus => DbPlanningOrchestration.PlanAsync(
                    _parameterization.IsContinuousRun,
                    dbParameterizationIndex[dbStatus.DbName],
                    dbStatus,
                    _sourceExportQueue,
                    ct))
                .ToImmutableArray();
            var exportingTasks = _dbStatusList
                .Select(dbStatus => DbExportingOrchestration.ExportAsync(
                    _parameterization.IsContinuousRun,
                    Task.WhenAll(planningTasks),
                    dbParameterizationIndex[dbStatus.DbName],
                    dbStatus,
                    _sourceExportQueue,
                    ct))
                .ToImmutableArray();
            //var stagingTasks = _dbStatusList
            //    .Select(dbStatus => DbStagingOrchestration.ExportAsync(
            //        _parameterization.IsContinuousRun,
            //        Task.WhenAll(exportingTasks),
            //        dbParameterizationIndex[dbStatus.DbName],
            //        dbStatus,
            //        _sourceExportQueue,
            //        ct))
            //    .ToImmutableArray();
            var allTasks = setupTasks
                .Concat(planningTasks)
                .Concat(exportingTasks);
                //.Concat(stagingTasks);

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