using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using KustoCopyFoundation;
using KustoCopyFoundation.KustoQuery;
using KustoCopySpecific.Bookmarks.Root;
using KustoCopySpecific.Parameters;
using KustoCopySpecific.Pipelines;
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
        private readonly RootBookmark _rootBookmark;
        private readonly ClusterExportPipeline _clusterExportPipeline;

        private CopyOrchestration(
            IAsyncDisposable blobLock,
            RootBookmark rootBookmark,
            ClusterExportPipeline exportPipeline)
        {
            _blobLock = blobLock;
            _rootBookmark = rootBookmark;
            _clusterExportPipeline = exportPipeline;
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
                var rootBookmark = await RootBookmark.RetrieveAsync(
                    folderClient.GetFileClient("root.bookmark"),
                    adlsCredential,
                    parameterization);

                if (!rootBookmark.Parameterization!.Equals(parameterization))
                {
                    throw new CopyException(
                        "Parameters can't be different from one run to "
                        + "another in the same data lake folder");
                }

                Trace.WriteLine("Connecting to source cluster...");

                var kustoBuilder = CreateKustoCredentials(
                    authenticationMode,
                    parameterization.Source!.ClusterQueryUri!);
                var sourceKustoClient = new KustoQueuedClient(
                    kustoBuilder,
                    parameterization.Source!.ConcurrentQueryCount,
                    parameterization.Source!.ConcurrentExportCommandCount);
                var exportPipeline = await ClusterExportPipeline.CreateAsync(
                    folderClient,
                    adlsCredential,
                    sourceKustoClient,
                    parameterization);

                return new CopyOrchestration(
                    blobLock,
                    rootBookmark,
                    exportPipeline);
            }
            catch
            {
                await blobLock.DisposeAsync();
                throw;
            }
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
            switch(authenticationMode)
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
            var exportTask = _clusterExportPipeline.RunAsync();

            await Task.WhenAll(exportTask);
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