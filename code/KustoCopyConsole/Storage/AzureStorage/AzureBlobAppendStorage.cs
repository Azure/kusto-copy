using Azure;
using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage.Files.DataLake.Specialized;
using Polly;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobAppendStorage : IAppendStorage
    {
        private static readonly AsyncPolicy _writeBlockRetryPolicy =
            Policy.Handle<RequestFailedException>()
            .RetryAsync(3);

        private readonly DataLakeFileClient _fileClient;
        private readonly AppendBlobClient _blobClient;
        private readonly BlobLock _blobLock;
        private int _writeCount;

        #region Constructors
        private AzureBlobAppendStorage(
            DataLakeFileClient fileClient,
            AppendBlobClient blobClient,
            BlobLock blobLock)
        {
            _fileClient = fileClient;
            _blobClient = blobClient;
            _blobLock = blobLock;
            _writeCount = _blobClient.AppendBlobMaxBlocks;
        }

        public async static Task<AzureBlobAppendStorage> CreateAsync(
            Uri directoryUri,
            string blobName,
            TokenCredential credential,
            CancellationToken ct)
        {
            var builder = new UriBuilder(directoryUri);

            builder.Query = string.Empty;
            builder.Path += builder.Path.EndsWith('/')
                ? blobName
                : $"/{blobName}";

            var fileClient = new DataLakeFileClient(builder.Uri, credential);
            var blobClient = new AppendBlobClient(builder.Uri, credential);

            await blobClient.CreateIfNotExistsAsync(new AppendBlobCreateOptions(), ct);

            var blobLock = await BlobLock.CreateAsync(blobClient, ct);

            return new AzureBlobAppendStorage(fileClient, blobClient, blobLock);
        }
        #endregion

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        int IAppendStorage.MaxBufferSize => _blobClient.AppendBlobMaxAppendBlockBytes;

        bool IAppendStorage.IsCompactionRequired => _writeCount >= _blobClient.AppendBlobMaxBlocks;

        async Task IAppendStorage.AtomicAppendAsync(byte[] buffer, CancellationToken ct)
        {
            await _writeBlockRetryPolicy.ExecuteAsync(async () =>
            {
                using (var stream = new MemoryStream(buffer.ToArray()))
                {
                    await _blobClient.AppendBlockAsync(stream, null, ct);
                }
            });
        }

        async Task IAppendStorage.AtomicReplaceAsync(byte[] buffer, CancellationToken ct)
        {
            const string TEMP_SUFFIX = ".tmp";

            //  Write to temp blob
            var tempBlobClient = _blobClient
                .GetParentBlobContainerClient()
                .GetAppendBlobClient($"{_blobClient.Name}{TEMP_SUFFIX}");

            await tempBlobClient.DeleteIfExistsAsync();
            await tempBlobClient.CreateIfNotExistsAsync();

            var remainingBuffer = buffer.Skip(0);

            _writeCount = 0;
            while (remainingBuffer.Any())
            {
                var currentBuffer =
                    remainingBuffer.Take(tempBlobClient.AppendBlobMaxAppendBlockBytes);

                using (var stream = new MemoryStream(currentBuffer.ToArray()))
                {
                    await tempBlobClient.AppendBlockAsync(stream, null, ct);
                }
                remainingBuffer =
                    remainingBuffer.Skip(tempBlobClient.AppendBlobMaxAppendBlockBytes);
                ++_writeCount;
            }

            //  Flip the temp to permanent blob by move / rename blob
            var tempFileClient = _fileClient
                .GetParentDirectoryClient()
                .GetFileClient($"{_fileClient.Name}{TEMP_SUFFIX}");

            await _writeBlockRetryPolicy.ExecuteAsync(async () =>
            {
                await tempFileClient.RenameAsync(
                    _fileClient.Path,
                    destinationConditions: new DataLakeRequestConditions
                    {
                        LeaseId = _blobLock.LeaseClient.LeaseId
                    });
            });
        }

        async Task<byte[]> IAppendStorage.LoadAllAsync(CancellationToken ct)
        {
            var result = await _blobClient.DownloadContentAsync(ct);
            var buffer = result.Value.Content.ToMemory().ToArray();

            return buffer;
        }
    }
}