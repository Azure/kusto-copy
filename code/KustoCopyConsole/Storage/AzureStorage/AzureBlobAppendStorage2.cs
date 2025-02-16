using Azure;
using Azure.Storage.Blobs.Specialized;
using Polly;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobAppendStorage2 : IAppendStorage2
    {
        private static readonly AsyncPolicy _writeBlockRetryPolicy =
            Policy.Handle<RequestFailedException>()
            .RetryAsync(3);

        private readonly AppendBlobClient _appendBlobClient;
        private int _writeCount = 0;

        public AzureBlobAppendStorage2(AppendBlobClient blobClient)
        {
            _appendBlobClient = blobClient;
        }

        int IAppendStorage2.MaxBufferSize => _appendBlobClient.AppendBlobMaxAppendBlockBytes;

        async Task<bool> IAppendStorage2.AtomicAppendAsync(
            IEnumerable<byte> content,
            CancellationToken ct)
        {
            if (_writeCount == _appendBlobClient.AppendBlobMaxAppendBlockBytes)
            {
                return false;
            }
            else
            {
                await _writeBlockRetryPolicy.ExecuteAsync(async () =>
                {
                    using (var stream = new MemoryStream(content.ToArray()))
                    {
                        await _appendBlobClient.AppendBlockAsync(stream, null, ct);
                    }
                });

                return true;
            }
        }
    }
}