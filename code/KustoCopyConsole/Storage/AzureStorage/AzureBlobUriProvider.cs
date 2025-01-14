using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Sas;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobUriProvider : IStagingBlobUriProvider
    {
        private static readonly TimeSpan WRITE_TIME_OUT = TimeSpan.FromMinutes(90);

        private readonly ImmutableArray<DataLakeFileSystemClient> _containerClient;

        public AzureBlobUriProvider(
            IImmutableList<Uri> stagingStorageContainers,
            TokenCredential credential)
        {
            new BlobServiceClient()
            _containerClient = stagingStorageContainers
                .Select(u => new DataLakeFileSystemClient(u, credential))
                .ToImmutableArray();
        }

        IEnumerable<Uri> IStagingBlobUriProvider.GetWritableRootUris(string path)
        {
            return _containerClient
                .Select(c => GetWritableRootUri(c, path));
        }

        Uri IStagingBlobUriProvider.AuthorizeUri(Uri uri)
        {
            throw new NotImplementedException();
        }

        private static Uri GetWritableRootUri(
            DataLakeFileSystemClient containerClient,
            string path)
        {
            var directoryClient = containerClient.GetDirectoryClient(path);
            var sas = directoryClient.GenerateSasUri(
                DataLakeSasPermissions.Write,
                DateTimeOffset.Now.Add(WRITE_TIME_OUT));

            throw new NotImplementedException();
        }
    }
}