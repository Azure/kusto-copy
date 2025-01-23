using Azure.Core;
using Azure.Storage.Blobs.Specialized;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobAppendStorage : IAppendStorage
    {
        private AppendBlobClient _blobClient;

        public AzureBlobAppendStorage(
            Uri directoryUri,
            string blobName,
            TokenCredential credential)
        {
            var builder = new UriBuilder(directoryUri);

            builder.Query = string.Empty;
            builder.Path += $"/{blobName}";

            _blobClient = new AppendBlobClient(builder.Uri, credential);
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

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            throw new NotImplementedException();
        }

        Task<byte[]> IAppendStorage.LoadAllAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}