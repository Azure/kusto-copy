using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using KustoCopyFoundation.Concurrency;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KustoCopyFoundation.Bookmarks
{
    public class BookmarkGateway
    {
        #region Inner Types
        private class BlockInfo
        {
            public BlockInfo(int id, int size)
            {
                Id = id;
                Size = size;
            }

            public int Id { get; }

            public int Size { get; }
        }

        private class BookmarkHeader
        {
            public Version BookmarkVersion { get; set; } = new Version(0, 1);

            public Version AppVersion { get; set; } = new Version();
        }

        private class CommitItem
        {
            public CommitItem(
                IEnumerable<BlockInfo> blocksToAdd,
                IEnumerable<int> blockIdsToRemove,
                IEnumerable<(int oldId, BlockInfo newBlock)> blocksToUpdate)
            {
                BlocksToAdd = blocksToAdd.ToImmutableArray();
                BlockIdsToRemove = blockIdsToRemove.ToImmutableArray();
                BlocksToUpdate = blocksToUpdate.ToImmutableArray();
            }

            public IImmutableList<BlockInfo> BlocksToAdd { get; }

            public IImmutableList<int> BlockIdsToRemove { get; }

            public IImmutableList<(int oldId, BlockInfo newBlock)> BlocksToUpdate { get; }

            public bool HasCommitted { get; set; } = false;
        }
        #endregion

        private readonly BlockBlobClient _blobClient;
        private readonly ExecutionQueue _executionQueue = new ExecutionQueue(1);
        private readonly ConcurrentStack<CommitItem> _commitItems =
            new ConcurrentStack<CommitItem>();
        private readonly ConcurrentStack<int> _idHoles;
        private readonly List<BlockInfo> _blocks = new List<BlockInfo>();
        private volatile int _nextBlockId = 0;

        private BookmarkGateway(BlockBlobClient blobClient, IEnumerable<BlockInfo> blocks)
        {
            _blobClient = blobClient;
            _blocks.AddRange(blocks);
            _nextBlockId = _blocks.Any() ? _blocks.Select(b => b.Id).Max() + 1 : 0;

            var possibleIds = Enumerable.Range(0, _nextBlockId);
            var unusedIds = possibleIds.Except(_blocks.Select(b => b.Id));

            _idHoles = new ConcurrentStack<int>(unusedIds);
        }

        public static async Task<BookmarkGateway> CreateAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential,
            bool shouldExist)
        {
            var blobClient = new BlockBlobClient(fileClient.Uri, credential);
            var exist = (await fileClient.ExistsAsync()).Value;

            if (!exist)
            {
                if (shouldExist)
                {
                    throw new CopyException($"Blob doesn't exist:  '${blobClient.Uri}'");
                }
                else
                {
                    await fileClient.CreateAsync();

                    return new BookmarkGateway(blobClient, ImmutableArray<BlockInfo>.Empty);
                }
            }
            else
            {
                var blockLists = await blobClient.GetBlockListAsync(BlockListTypes.Committed);
                var blocks = blockLists
                    .Value
                    .CommittedBlocks
                    .Select(b => new BlockInfo(DecodeId(b.Name), b.Size));

                return new BookmarkGateway(blobClient, blocks);
            }
        }

        public async Task<IImmutableList<BookmarkBlock>> ReadAllBlocksAsync()
        {
            var content = (await _blobClient.DownloadContentAsync()).Value.Content.ToMemory();
            var bookmarkBlockBuilder = ImmutableArray<BookmarkBlock>.Empty.ToBuilder();
            var offset = 0;

            foreach (var block in _blocks)
            {
                var buffer = content.Slice(offset, block.Size);
                var bookmarkBlock = new BookmarkBlock(block.Id, buffer);

                if (offset == 0)
                {   //  This is the header ; let's validate it
                    var header = JsonSerializer.Deserialize<BookmarkHeader>(buffer.Span);

                    if (header == null || header.BookmarkVersion != new BookmarkHeader().BookmarkVersion)
                    {
                        throw new CopyException($"Wrong header on ${_blobClient.Uri}");
                    }
                }
                else
                {
                    bookmarkBlockBuilder.Add(bookmarkBlock);
                }
                offset += block.Size;
            }

            return bookmarkBlockBuilder.ToImmutable();
        }

        public async Task<IImmutableList<BookmarkBlockValue<T>>> ReadAllBlockValuesAsync<T>()
        {
            var blocks = await ReadAllBlocksAsync();
            var values = blocks
                .Select(b => new BookmarkBlockValue<T>(
                    b.Id,
                    SerializationHelper.ToObject<T>(b.Buffer)))
                .ToImmutableArray();

            return values;
        }

        public async Task<BookmarkTransactionResult> ApplyTransactionAsync(
            BookmarkTransaction transaction)
        {
            var headerFunc = async () =>
            {
                var id = NewId();
                var stream = GetBookmarkHeaderStream();

                await _blobClient.StageBlockAsync(EncodeId(id), stream);

                return new BlockInfo(id, (int)stream.Length);
            };
            var headerTasks = !_blocks.Any() ? new[] { headerFunc() } : new Task<BlockInfo>[0];
            var createBlockFunc = async (ReadOnlyMemory<byte> buffer) =>
            {
                var id = NewId();

                await _blobClient.StageBlockAsync(EncodeId(id), new MemoryStream(buffer.ToArray()));

                return new BlockInfo(id, buffer.Length);
            };
            var addingTasks = transaction
                .AddingBlockBuffers
                .Select(b => createBlockFunc(b))
                .ToImmutableArray();
            var updatingComposites = transaction
                .UpdatingBlocks
                .Select(b => new { OldId = b.Id, Task = createBlockFunc(b.Buffer) })
                .ToImmutableArray();

            await Task.WhenAll(
                addingTasks
                .Concat(headerTasks)
                .Concat(updatingComposites.Select(t => t.Task)));

            var item = new CommitItem(
                headerTasks.Concat(addingTasks).Select(t => t.Result),
                transaction.DeletingBlockIds,
                updatingComposites.Select(c => (c.OldId, c.Task.Result)));

            await CommitTransactionAsync(item);

            //  Do not put headerBlockIds on purpose as this is implementation detail for this class
            return new BookmarkTransactionResult(
                addingTasks.Select(t => t.Result.Id),
                updatingComposites.Select(c => c.Task.Result.Id),
                transaction.DeletingBlockIds);
        }

        private async Task CommitTransactionAsync(CommitItem newItem)
        {   //  A bit of multithreading synchronization here
            //  Stack the item
            _commitItems.Push(newItem);

            await _executionQueue.RequestRunAsync(async () =>
            {
                if (!newItem.HasCommitted)
                {   //  First let's try to grab as many items as we can
                    var items = new List<CommitItem>();

                    while (_commitItems.Any())
                    {
                        CommitItem? item;

                        if (_commitItems.TryPop(out item))
                        {
                            items.Add(item);
                        }
                    }
                    //  Did we actually pick any item or they all got stolen by another thread?
                    if (items.Any())
                    {
                        await CommitItemsAsync(items);
                    }
                }
            });
        }

        private async Task CommitItemsAsync(IEnumerable<CommitItem> items)
        {
            var blocksToAdd = items
                .Select(i => i.BlocksToAdd)
                .SelectMany(i => i);
            var blockIdsToRemove = items
                .Select(i => i.BlockIdsToRemove)
                .SelectMany(i => i);
            var blockIdsToUpdate = items
                .Select(i => i.BlocksToUpdate)
                .SelectMany(i => i);

            if (blockIdsToRemove.Any() || blockIdsToUpdate.Any())
            {
                //  Make block id faster to manipulate
                var removeIdSet = blockIdsToRemove.ToHashSet();
                var updateIdMap = blockIdsToUpdate.ToDictionary(p => p.oldId, p => p.newBlock);

                for (int i = 0; i != _blocks.Count; i++)
                {
                    var block = _blocks[i];
                    BlockInfo? newBlock;

                    if (updateIdMap.TryGetValue(block.Id, out newBlock))
                    {
                        _blocks[i] = newBlock;
                    }
                    if (removeIdSet.Contains(block.Id))
                    {
                        _blocks.RemoveAt(i);
                        //  To compensate for incoming ++
                        --i;
                    }
                }
            }
            _blocks.AddRange(blocksToAdd);

            //  Actually commit the new block list to the blob
            await _blobClient.CommitBlockListAsync(_blocks.Select(b => EncodeId(b.Id)));

            //  Recover unused ids
            foreach (var id in blockIdsToRemove)
            {
                _idHoles.Push(id);
            }

            //  Release all threads waiting for the items
            foreach (var item in items)
            {
                item.HasCommitted = true;
            }
        }

        private int NewId()
        {
            int id;

            if (_idHoles.TryPop(out id))
            {
                return id;
            }
            else
            {
                return Interlocked.Increment(ref _nextBlockId);
            }
        }

        private static MemoryStream GetBookmarkHeaderStream()
        {
            return new MemoryStream(SerializationHelper.ToBytes(new BookmarkHeader()));
        }

        private static string EncodeId(int id)
        {
            var paddedId = id.ToString("D10");
            var buffer = UTF8Encoding.UTF8.GetBytes(paddedId);

            return Convert.ToBase64String(buffer);
        }

        private static int DecodeId(string base64)
        {
            var buffer = Convert.FromBase64String(base64);
            var paddedId = UTF8Encoding.UTF8.GetString(buffer);

            return int.Parse(paddedId);
        }
    }
}