using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Kusto.Data;
using KustoCopyFoundation;
using KustoCopyFoundation.KustoQuery;
using KustoCopySpecific.Bookmarks.DbExportStorage;
using KustoCopySpecific.Bookmarks.DbStorage;
using KustoCopySpecific.Bookmarks.ExportPlan;
using KustoCopySpecific.Bookmarks.IterationExportStorage;
using KustoCopySpecific.Parameters;
using KustoCopySpecific.Pipelines;
using System.Collections.Immutable;
using System.Diagnostics;

namespace kusto_copy
{
    internal class CopyOrchestration : IAsyncDisposable
    {
        #region Inner Types
        private class DataLakeFolder
        {
            public DataLakeFolder(string url)
            {
                Uri? folderUri;

                if (!Uri.TryCreate(url, UriKind.Absolute, out folderUri))
                {
                    throw new CopyException($"Data lake folder URL isn't a URL:  '{url}'");
                }
                else
                {
                    var scheme = folderUri.Scheme;
                    var host = folderUri.Host;
                    var dotIndex = host.IndexOf('.');

                    if (scheme != "https")
                    {
                        throw new CopyException(
                            $"Data lake folder URL should have scheme 'https':  '{url}'");
                    }
                    else if (dotIndex == -1 || host.Substring(dotIndex + 1) != "blob.core.windows.net")
                    {
                        throw new CopyException(
                            "Data lake folder URL should have "
                            + $"<account name>.blob.core.windows.net as host name ; '{url}'");
                    }
                    else if (folderUri.Query != string.Empty)
                    {
                        throw new CopyException(
                            $"Data lake folder URL can't have a query string:  '{url}'");
                    }
                    else
                    {
                        var localPath = folderUri.LocalPath;
                        var parts = localPath.Split('/');

                        if (parts.Length < 2)
                        {
                            throw new CopyException(
                                "Data lake folder URL needs at least a container in its path:  "
                                + $"'{url}'");
                        }
                        else
                        {
                            AccountName = host.Substring(0, dotIndex);
                            ContainerName = parts[1];
                            FolderPath = string.Join('/', parts.Skip(2));
                        }
                    }
                }
            }

            public string AccountName { get; }

            public string ContainerName { get; }

            public string FolderPath { get; }
        }
        #endregion

        private readonly IAsyncDisposable _blobLock;
        private readonly IImmutableList<DbExportPlanPipeline> _dbExportPlanPipelines;
        private readonly IImmutableList<DbExportExecutionPipeline> _dbExportExecutionPipelines;

        private CopyOrchestration(
            IAsyncDisposable blobLock,
            IEnumerable<DbExportPlanPipeline> dbExportPlanPipelines,
            IEnumerable<DbExportExecutionPipeline> dbExportExecutionPipelines)
        {
            _blobLock = blobLock;
            _dbExportPlanPipelines = dbExportPlanPipelines.ToImmutableArray();
            _dbExportExecutionPipelines = dbExportExecutionPipelines.ToImmutableArray();
        }

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            AuthenticationMode authenticationMode,
            string dataLakeFolderUrl,
            MainParameterization parameterization)
        {
            Trace.WriteLine("Connecting to Data Lake...");

            var adlsCredential = CreateAdlsCredentials(authenticationMode);
            var folder = new DataLakeFolder(dataLakeFolderUrl);
            var folderClient = await GetFolderClientAsync(
                dataLakeFolderUrl,
                adlsCredential,
                folder);
            var lockClient = folderClient.GetFileClient("lock");

            await lockClient.CreateIfNotExistsAsync();

            var blobLock = await BlobLock.CreateAsync(new BlobClient(lockClient.Uri, adlsCredential));

            if (blobLock == null)
            {
                throw new CopyException(
                    $"Can't acquire lock on '{lockClient.Uri}' ; "
                    + "is there another instance of the application running?");
            }
            try
            {
                Trace.WriteLine("Connecting to source cluster...");

                return await CreationOrchestrationAsync(
                    authenticationMode,
                    parameterization,
                    adlsCredential,
                    folderClient,
                    blobLock);
            }
            catch
            {
                await blobLock.DisposeAsync();
                throw;
            }
        }

        private static async Task<CopyOrchestration> CreationOrchestrationAsync(
            AuthenticationMode authenticationMode,
            MainParameterization parameterization,
            TokenCredential adlsCredential,
            DataLakeDirectoryClient folderClient,
            IAsyncDisposable blobLock)
        {
            var kustoBuilder = CreateKustoCredentials(
                authenticationMode,
                parameterization.Source!.ClusterQueryUri!);
            var sourceKustoClient = new KustoQueuedClient(
                kustoBuilder,
                parameterization.Source!.ConcurrentQueryCount,
                parameterization.Source!.ConcurrentExportCommandCount);
            var sourceFolderClient = folderClient.GetSubDirectoryClient("source");
            var rootTempFolderClient = folderClient.GetSubDirectoryClient("temp");
            //  Flush temp folder
            var flushTempFolderTask = rootTempFolderClient.DeleteIfExistsAsync();
            //  Fetch the database list from the cluster
            var allDbNames = await sourceKustoClient.ExecuteCommandAsync(
                KustoPriority.WildcardPriority,
                string.Empty,
                ".show databases | project DatabaseName",
                r => (string)r["DatabaseName"]);
            var databases = parameterization.Source!.Databases
                ?? allDbNames.Select(n => new SourceDatabaseParameterization
                {
                    Name = n
                }).ToImmutableArray();

            Trace.WriteLine($"Source databases:  {{{string.Join(", ", databases.OrderBy(n => n))}}}");
            await flushTempFolderTask;

            var pipelineTasks = databases
                .Select(async db => await CreateDbPipelinesAsync(
                    parameterization,
                    db,
                    adlsCredential,
                    sourceKustoClient,
                    sourceFolderClient,
                    rootTempFolderClient))
                .ToImmutableArray();

            await Task.WhenAll(pipelineTasks);

            return new CopyOrchestration(
                blobLock,
                pipelineTasks.Select(t => t.Result.dbExportPlan),
                pipelineTasks.Select(t => t.Result.dbExportExecution));
        }

        private static async Task<(DbExportPlanPipeline dbExportPlan, DbExportExecutionPipeline dbExportExecution)> CreateDbPipelinesAsync(
            MainParameterization parameterization,
            SourceDatabaseParameterization db,
            TokenCredential adlsCredential,
            KustoQueuedClient sourceKustoClient,
            DataLakeDirectoryClient sourceFolderClient,
            DataLakeDirectoryClient rootTempFolderClient)
        {
            var dbConfig = parameterization.Source!.DatabaseDefault.Override(
                parameterization,
                db.DatabaseOverrides);
            var dbFolderClient = sourceFolderClient.GetSubDirectoryClient(db.Name);
            var planFileClient = dbFolderClient.GetFileClient("plan-db.bookmark");
            var dbExportPlanBookmark = await DbExportPlanBookmark.RetrieveAsync(
                planFileClient,
                adlsCredential);
            var dbFileClient = dbFolderClient.GetFileClient("db.bookmark");
            var dbStorageBookmark = await DbStorageBookmark.RetrieveAsync(
                dbFileClient,
                adlsCredential,
                dbConfig);
            var storageFileClient = dbFolderClient.GetFileClient("db-storage.bookmark");
            var dbExportStorageBookmark = await DbExportStorageBookmark.RetrieveAsync(
                storageFileClient,
                adlsCredential);
            var iterationFederation = new DbIterationStorageFederation(
                dbExportStorageBookmark,
                dbFolderClient,
                adlsCredential);
            var dbExportPlan = new DbExportPlanPipeline(
                db.Name!,
                dbStorageBookmark,
                dbExportPlanBookmark,
                sourceKustoClient,
                dbConfig.MaxRowsPerTablePerIteration);
            var dbExportExecution = new DbExportExecutionPipeline(
                rootTempFolderClient,
                db.Name!,
                dbStorageBookmark,
                dbExportPlanBookmark,
                iterationFederation,
                sourceKustoClient,
                parameterization.Source!.ConcurrentExportCommandCount);

            return (dbExportPlan, dbExportExecution);
        }

        private static KustoConnectionStringBuilder CreateKustoCredentials(
            AuthenticationMode authenticationMode,
            string clusterQueryUrl)
        {
            var builder = new KustoConnectionStringBuilder(clusterQueryUrl);

            switch (authenticationMode)
            {
                case AuthenticationMode.AppSecret:
                    throw new NotSupportedException();
                case AuthenticationMode.AzCli:
                    return builder.WithAadAzCliAuthentication();
                case AuthenticationMode.Browser:
                    return builder.WithAadUserPromptAuthentication();

                default:
                    throw new NotSupportedException(
                        $"Unsupported authentication mode '{authenticationMode}'");
            }
        }

        private static TokenCredential CreateAdlsCredentials(
            AuthenticationMode authenticationMode)
        {
            switch (authenticationMode)
            {
                case AuthenticationMode.AppSecret:
                    throw new NotSupportedException();
                case AuthenticationMode.AzCli:
                    return new AzureCliCredential();
                case AuthenticationMode.Browser:
                    return new InteractiveBrowserCredential(new InteractiveBrowserCredentialOptions
                    {
                        TokenCachePersistenceOptions = new TokenCachePersistenceOptions()
                    });

                default:
                    throw new NotSupportedException(
                        $"Unsupported authentication mode '{authenticationMode}'");
            }
        }

        public async Task RunAsync()
        {
            var planTasks = _dbExportPlanPipelines
                .Select(p => p.RunAsync())
                .ToImmutableArray();
            var executionTasks = _dbExportExecutionPipelines
                .Select(p => p.RunAsync())
                .ToImmutableArray();

            await Task.WhenAll(planTasks.Concat(executionTasks));
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _blobLock.DisposeAsync();
        }

        private static async Task<DataLakeDirectoryClient> GetFolderClientAsync(
            string dataLakeFolderUrl,
            TokenCredential credential,
            DataLakeFolder folder)
        {
            var dfsUrl = $"https://{folder.AccountName}.blob.core.windows.net";
            var lakeClient = new DataLakeServiceClient(new Uri(dfsUrl), credential);
            var containerClient = lakeClient.GetFileSystemClient(folder.ContainerName);
            var containerExist = (await containerClient.ExistsAsync()).Value;
            var folderClient = containerClient.GetDirectoryClient(folder.FolderPath);

            if (!containerExist)
            {
                throw new CopyException(
                    "Data lake folder URL points to non-existing container:  "
                    + $"'{dataLakeFolderUrl}'");
            }
            else
            {
                await folderClient.CreateIfNotExistsAsync();

                var folderProperties = (await folderClient.GetPropertiesAsync()).Value;

                if (!folderProperties.IsDirectory)
                {
                    throw new CopyException(
                        "Data lake folder URL points to a blob instead of a folder:  "
                        + $"'{dataLakeFolderUrl}'");
                }
            }

            return folderClient;
        }
    }
}