using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobAppendStorage : IAppendStorage
    {
        private AppendBlobClient _blobClient;
        private readonly BlobLock _blobLock;

        #region Constructors
        private AzureBlobAppendStorage(AppendBlobClient blobClient, BlobLock blobLock)
        {
            _blobClient = blobClient;
            _blobLock = blobLock;
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

            var blobClient = new AppendBlobClient(builder.Uri, credential);

            await blobClient.CreateIfNotExistsAsync(new AppendBlobCreateOptions(), ct);

            var blobLock = await BlobLock.CreateAsync(blobClient, ct);

            return new AzureBlobAppendStorage(blobClient, blobLock);
        }
        #endregion

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        int IAppendStorage.MaxBufferSize => 4194304;

        Task<bool> IAppendStorage.AtomicAppendAsync(byte[] buffer, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        Task IAppendStorage.AtomicReplaceAsync(byte[] buffer, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        Task<byte[]> IAppendStorage.LoadAllAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}