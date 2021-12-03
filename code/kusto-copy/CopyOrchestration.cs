using Azure.Core;
using Azure.Identity;
using Azure.Storage.Files.DataLake;

namespace kusto_copy
{
    internal class CopyOrchestration
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

        private readonly DataLakeDirectoryClient _folderClient;

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            string dataLakeFolderUrl,
            Uri sourceClusterUri)
        {
            var credential = new InteractiveBrowserCredential();
            var folder = new DataLakeFolder(dataLakeFolderUrl);
            var folderClient = await GetFolderClientAsync(dataLakeFolderUrl, credential, folder);

            return new CopyOrchestration(folderClient);
        }

        private CopyOrchestration(DataLakeDirectoryClient folderClient)
        {
            _folderClient = folderClient;
        }

        private static async Task<DataLakeDirectoryClient> GetFolderClientAsync(
            string dataLakeFolderUrl,
            TokenCredential credential,
            DataLakeFolder folder)
        {
            var dfsUrl = $"https://{folder.AccountName}.dfs.core.windows.net";
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
                var folderExists = (await folderClient.ExistsAsync()).Value;

                if (!folderExists)
                {
                    throw new CopyException(
                        "Data lake folder URL points to non-existing folder:  "
                        + $"'{dataLakeFolderUrl}'");
                }
                else
                {
                    var folderProperties = (await folderClient.GetPropertiesAsync()).Value;

                    if (!folderProperties.IsDirectory)
                    {
                        throw new CopyException(
                            "Data lake folder URL points to a blob instead of a folder:  "
                            + $"'{dataLakeFolderUrl}'");
                    }
                }
            }

            return folderClient;
        }
    }
}