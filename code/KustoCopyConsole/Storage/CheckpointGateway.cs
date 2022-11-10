using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal class CheckpointGateway
    {
        private const string TEMP_CHECKPOINT_BLOB = "temp-index.csv";

        private readonly AppendBlobClient _blobClient;
        private readonly TokenCredential _credential;
        private volatile int _blockCount;

        #region Constructors
        public CheckpointGateway(Uri blobUri, TokenCredential credential)
            : this(blobUri, credential, 0)
        {
        }

        private CheckpointGateway(Uri blobUri, TokenCredential credential, int blockCount)
        {
            var builder = new BlobUriBuilder(blobUri);

            //  Enforce blob storage API
            builder.Host =
                builder.Host.Replace(".dfs.core.windows.net", ".blob.core.windows.net");
            blobUri = builder.ToUri();

            _blobClient = new AppendBlobClient(blobUri, credential);
            _credential = credential;
            _blockCount = blockCount;
        }
        #endregion

        public Uri BlobUri => _blobClient.Uri;

        public bool CanWrite => _blockCount < 50000;

        public async Task<bool> ExistsAsync(CancellationToken ct)
        {
            return await _blobClient.ExistsAsync(ct);
        }

        private async Task DeleteIfExistsAsync(CancellationToken ct)
        {
            await _blobClient.DeleteIfExistsAsync(cancellationToken: ct);
        }

        public async Task CreateAsync(CancellationToken ct)
        {
            await _blobClient.CreateAsync(new AppendBlobCreateOptions(), ct);
        }

        public async Task<CheckpointGateway> GetTemporaryCheckpointGatewayAsync(
            CancellationToken ct)
        {
            var tempSegments = _blobClient.Uri.Segments
                .Take(_blobClient.Uri.Segments.Length - 1)
                .Append(TEMP_CHECKPOINT_BLOB);
            var tempPath = string.Join(string.Empty, tempSegments);
            var tempBlobUrl = new Uri($"https://{_blobClient.Uri.Authority}{tempPath}");
            var tempCheckpointGateway = new CheckpointGateway(tempBlobUrl, _credential);

            await tempCheckpointGateway.DeleteIfExistsAsync(ct);
            await tempCheckpointGateway.CreateAsync(ct);

            return tempCheckpointGateway;
        }

        public async Task<byte[]> ReadAllContentAsync(CancellationToken ct)
        {
            var result = await _blobClient.DownloadContentAsync(ct);
            var buffer = result.Value.Content.ToArray();

            return buffer;
        }

        public async Task WriteAsync(byte[] buffer, CancellationToken ct)
        {
            using (var stream = new MemoryStream(buffer))
            {
                await _blobClient.AppendBlockAsync(stream, cancellationToken: ct);
            }
            Interlocked.Increment(ref _blockCount);
        }

        public async Task<CheckpointGateway> MoveAsync(Uri destinationUri, CancellationToken ct)
        {
            var currentFile = new DataLakeFileClient(_blobClient.Uri, _credential);
            var destinationFile = new DataLakeFileClient(destinationUri);

            await currentFile.RenameAsync(destinationFile.Path, cancellationToken: ct);

            return new CheckpointGateway(destinationUri, _credential, _blockCount);
        }
    }
}