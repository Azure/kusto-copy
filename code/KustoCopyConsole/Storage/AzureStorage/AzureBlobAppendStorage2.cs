using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Kusto.Cloud.Platform.Utils;
using Polly;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobAppendStorage2 : IAppendStorage2
    {
        private readonly AppendBlobClient _appendBlobClient;
        private readonly AsyncPolicy _writeBlockRetryPolicy;
        private int _writeCount = 0;

        public AzureBlobAppendStorage2(
            AppendBlobClient blobClient,
            AsyncPolicy writeBlockRetryPolicy)
        {
            _appendBlobClient = blobClient;
            _writeBlockRetryPolicy = writeBlockRetryPolicy;
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
                    if (_writeCount == 0)
                    {
                        await _appendBlobClient.DeleteIfExistsAsync();
                        await _appendBlobClient.CreateAsync();
                    }
                    using (var stream = new MemoryStream(content.ToArray()))
                    {
                        await _appendBlobClient.AppendBlockAsync(stream, null, ct);
                        ++_writeCount;
                    }
                });

                return true;
            }
        }
    }
}