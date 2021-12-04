using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KustoCopyBlobs
{
    internal class BookmarkGateway
    {
        #region Inner Types
        private class FakeLock : IAsyncDisposable
        {
            ValueTask IAsyncDisposable.DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        private class BookmarkHeader
        {
            public Version BookmarkVersion { get; set; } = new Version(0, 1);

            public Version AppVersion { get; set; } = new Version();
        }
        #endregion

        #region Public Inner Types
        public class BookmarkBlock
        {
            public BookmarkBlock(string id, ReadOnlyMemory<byte> buffer)
            {
                Id = id;
                Buffer = buffer;
            }

            public string Id { get; }

            public ReadOnlyMemory<byte> Buffer { get; }
        }
        #endregion

        private const string HEADER_ID = "header";
        private readonly DataLakeFileClient _fileClient;
        private readonly BlockBlobClient _blobClient;
        private readonly bool _shouldExist;
        private bool _hasExistBeenTested = false;
        private IImmutableList<string>? _blockIds;

        public BookmarkGateway(DataLakeFileClient fileClient, TokenCredential credential, bool shouldExist)
        {
            _fileClient = fileClient;
            _blobClient = new BlockBlobClient(
                fileClient.Uri,
                credential);
            _shouldExist = shouldExist;
        }

        public async Task<IAsyncDisposable> PermanentLockAsync()
        {
            await EnsureExistAsync();
            await ValueTask.CompletedTask;

            return new FakeLock();
        }

        public async Task<IImmutableList<BookmarkBlock>> ReadAllBlocksAsync()
        {
            await EnsureExistAsync();

            var blockListTask = _blobClient.GetBlockListAsync(BlockListTypes.Committed);
            var contentTask = _blobClient.DownloadContentAsync();
            var blockList = (await blockListTask).Value.CommittedBlocks;
            var content = (await contentTask).Value.Content.ToMemory();
            var builder = ImmutableArray<BookmarkBlock>.Empty.ToBuilder();
            var offset = 0;

            foreach (var block in blockList)
            {
                var id = block.Name;
                var buffer = content.Slice(offset, block.Size);
                var bookmarkBlock = new BookmarkBlock(id, buffer);

                if (offset == 0)
                {   //  This is the header ; let's validate it
                    var header = JsonSerializer.Deserialize<BookmarkHeader>(buffer.Span);

                    if (header == null || header.BookmarkVersion != new BookmarkHeader().BookmarkVersion)
                    {
                        throw new CopyException($"Wrong header on ${_fileClient.Uri}");
                    }
                }
                else
                {
                    builder.Add(bookmarkBlock);
                }
                offset += block.Size;
            }
            _blockIds = blockList
                .Select(b => b.Name)
                .ToImmutableArray();

            return builder.ToImmutable();
        }

        private async Task EnsureExistAsync()
        {
            if (!_hasExistBeenTested)
            {
                var exist = (await _fileClient.ExistsAsync()).Value;

                if (!exist)
                {
                    if (_shouldExist)
                    {
                        throw new CopyException($"Blob doesn't exist:  '${_fileClient.Uri}'");
                    }
                    else
                    {
                        await _fileClient.CreateAsync();
                    }
                }
                _hasExistBeenTested = true;
            }
        }
    }
}