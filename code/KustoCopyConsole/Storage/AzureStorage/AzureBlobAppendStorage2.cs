using Azure;
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

        #region Constructors
        private AzureBlobAppendStorage2(
            AppendBlobClient blobClient,
            AsyncPolicy writeBlockRetryPolicy)
        {
            _appendBlobClient = blobClient;
            _writeBlockRetryPolicy = writeBlockRetryPolicy;
        }

        public static async Task<AzureBlobAppendStorage2> CreateAsync(
            AppendBlobClient blobClient,
            AsyncPolicy writeBlockRetryPolicy)
        {
            await blobClient.DeleteIfExistsAsync();
            await blobClient.CreateAsync();

            return new AzureBlobAppendStorage2(blobClient, writeBlockRetryPolicy);
        }
        #endregion

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