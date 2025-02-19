using Azure;
using Azure.Core;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Polly;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobFileSystem : IFileSystem
    {
        private static readonly AsyncPolicy _writeBlockRetryPolicy =
            Policy.Handle<RequestFailedException>()
            .RetryAsync(3);

        private readonly DataLakeDirectoryClient _rootDirectory;
        private readonly TokenCredential _tokenCredential;

        /// <summary>Construct an Azure Blob based <see cref="IFileSystem"/>.</summary>
        /// <param name="dataLakeRootUrl">Root directory of the file system.</param>
        public AzureBlobFileSystem(string dataLakeRootUrl, TokenCredential tokenCredential)
        {
            _rootDirectory = new DataLakeDirectoryClient(
                new Uri(dataLakeRootUrl),
                tokenCredential);
            _tokenCredential = tokenCredential;
        }

        async Task<Stream?> IFileSystem.OpenReadAsync(string path, CancellationToken ct)
        {
            var fileClient = _rootDirectory.GetFileClient(path);

            if (await fileClient.ExistsAsync(ct))
            {
                return await fileClient.OpenReadAsync(new DataLakeOpenReadOptions(false), ct);
            }
            else
            {
                return null;
            }
        }

        async Task<IAppendStorage2> IFileSystem.OpenWriteAsync(string path, CancellationToken ct)
        {
            var fileClient = _rootDirectory.GetFileClient(path);

            if (await fileClient.ExistsAsync(ct))
            {
                throw new InvalidOperationException($"Blob '{fileClient.Uri}' already exists");
            }
            else
            {
                var blobClient = new AppendBlobClient(fileClient.Uri, _tokenCredential);
                var storage = new AzureBlobAppendStorage2(
                    blobClient,
                    _writeBlockRetryPolicy);

                return storage;
            }
        }

        async Task IFileSystem.MoveAsync(string source, string destination, CancellationToken ct)
        {
            var sourceFile = _rootDirectory.GetFileClient(source);
            var destinationFile = _rootDirectory.GetFileClient(destination);

            await _writeBlockRetryPolicy.ExecuteAsync(async () =>
            {
                await sourceFile.RenameAsync(destinationFile.Path);
            });
        }

        async Task IFileSystem.RemoveFolderAsync(string path, CancellationToken ct)
        {
            var subDirectory = _rootDirectory.GetSubDirectoryClient(path);

            await subDirectory.DeleteIfExistsAsync();
        }
    }
}