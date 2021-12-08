using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Net.Client;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Parameters;
using KustoCopyBookmarks.Root;

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

        private readonly RootBookmark _rootBookmark;
        private readonly IAsyncDisposable _blobLock;

        private CopyOrchestration(RootBookmark rootBookmark, IAsyncDisposable blobLock)
        {
            _rootBookmark = rootBookmark;
            _blobLock = blobLock;
        }

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            string dataLakeFolderUrl,
            MainParameterization parameterization)
        {
            var credential = new InteractiveBrowserCredential();
            var folder = new DataLakeFolder(dataLakeFolderUrl);
            var folderClient = await GetFolderClientAsync(dataLakeFolderUrl, credential, folder);
            var lockClient = folderClient.GetFileClient("lock");

            await lockClient.CreateIfNotExistsAsync();

            var lockBlob = await BlobLock.CreateAsync(new BlobClient(lockClient.Uri, credential));

            if (lockBlob == null)
            {
                throw new CopyException(
                    $"Can't acquire lock on '{lockClient.Uri}' ; "
                    + "is there another instance of the application running?");
            }
            try
            {
                var rootBookmark = await RootBookmark.RetrieveAsync(
                    folderClient.GetFileClient("root.bookmark"),
                    credential);

                if (rootBookmark.Parameterization == null)
                {
                    await rootBookmark.SetParameterizationAsync(parameterization);
                }
                else if (!rootBookmark.Parameterization!.Equals(parameterization))
                {
                    throw new CopyException(
                        "Parameters can't be different from one run to "
                        + "the other in the same data lake folder");
                }

                var clusterQueryUri =
                    ValidateClusterQueryUri(parameterization.Source!.ClusterQueryUri!);
                var builder = new KustoConnectionStringBuilder(clusterQueryUri.ToString())
                    .WithAadUserPromptAuthentication();
                var commandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);

                return new CopyOrchestration(rootBookmark, lockBlob);
            }
            catch
            {
                await lockBlob.DisposeAsync();
                throw;
            }
        }

        public async Task RunAsync()
        {
            await ValueTask.CompletedTask;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _blobLock.DisposeAsync();
        }

        private static Uri ValidateClusterQueryUri(string clusterQueryUrl)
        {
            Uri? clusterUri;

            if (Uri.TryCreate(clusterQueryUrl, UriKind.Absolute, out clusterUri))
            {
                return clusterUri;
            }
            else
            {
                throw new CopyException($"Invalid cluster query uri:  '{clusterQueryUrl}'");
            }
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