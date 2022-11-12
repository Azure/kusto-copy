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
        private const string CHECKPOINT_BLOB = "index.csv";
        private const string TEMP_CHECKPOINT_BLOB = "temp-index.csv";

        private readonly DataLakeDirectoryClient _lakeFolderClient;
        private readonly AppendBlobClient _blobClient;
        private volatile int _blockCount;

        #region Constructors
        public CheckpointGateway(
            DataLakeDirectoryClient lakeFolderClient,
            BlobContainerClient lakeContainerClient)
            : this(lakeFolderClient, lakeContainerClient, CHECKPOINT_BLOB, 0)
        {
        }

        private CheckpointGateway(
            DataLakeDirectoryClient lakeFolderClient,
            BlobContainerClient lakeContainerClient,
            string blobName,
            int blockCount)
        {
            _lakeFolderClient = lakeFolderClient;
            _blobClient = lakeContainerClient.GetAppendBlobClient(
                $"{lakeFolderClient.Path}/{blobName}");
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
            var tempCheckpointGateway = new CheckpointGateway(
                _lakeFolderClient,
                _blobClient.GetParentBlobContainerClient(),
                TEMP_CHECKPOINT_BLOB,
                0);

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

        public async Task<CheckpointGateway> MoveOutOfTemporaryAsync(CancellationToken ct)
        {
            var currentFile =
                _lakeFolderClient.GetFileClient(_blobClient.Uri.Segments.Last());
            var destinationFile =
                _lakeFolderClient.GetFileClient(CHECKPOINT_BLOB);

            await currentFile.RenameAsync(destinationFile.Path, cancellationToken: ct);

            return new CheckpointGateway(
                _lakeFolderClient,
                _blobClient.GetParentBlobContainerClient(),
                CHECKPOINT_BLOB,
                _blockCount);
        }
    }
}