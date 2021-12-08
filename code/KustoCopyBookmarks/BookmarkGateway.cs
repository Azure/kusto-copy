using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KustoCopyBookmarks
{
    internal partial class BookmarkGateway
    {
        #region Inner Types
        private class BookmarkHeader
        {
            public Version BookmarkVersion { get; set; } = new Version(0, 1);

            public Version AppVersion { get; set; } = new Version();
        }

        private class CommitItem
        {
            private readonly TaskCompletionSource _taskCompletionSource = new TaskCompletionSource();

            public CommitItem(
                IEnumerable<int> blockIdsToAdd,
                IEnumerable<int> blockIdsToRemove)
            {
                BlockIdsToAdd = blockIdsToAdd.ToImmutableArray();
                BlockIdsToRemove = blockIdsToRemove.ToImmutableArray();
                Task = _taskCompletionSource.Task;
            }

            public IImmutableList<int> BlockIdsToAdd { get; }

            public IImmutableList<int> BlockIdsToRemove { get; }

            public Task Task { get; }

            public void CompleteItem()
            {
                _taskCompletionSource.SetResult();
            }
        }
        #endregion

        private readonly object _alterBlockIdsLock = new object();
        private readonly DataLakeFileClient _fileClient;
        private readonly BlockBlobClient _blobClient;
        private readonly bool _shouldExist;
        private readonly ConcurrentStack<CommitItem> _commitItems =
            new ConcurrentStack<CommitItem>();
        private bool _hasExistBeenTested = false;
        private IImmutableList<int>? _blockIds;
        private volatile int _nextBlockId = 0;
        private volatile Task _commitingTask = Task.CompletedTask;

        public BookmarkGateway(DataLakeFileClient fileClient, TokenCredential credential, bool shouldExist)
        {
            _fileClient = fileClient;
            _blobClient = new BlockBlobClient(fileClient.Uri, credential);
            _shouldExist = shouldExist;
        }

        public async Task<IImmutableList<BookmarkBlock>> ReadAllBlocksAsync()
        {
            await EnsureExistAsync();

            var blockListTask = _blobClient.GetBlockListAsync(BlockListTypes.Committed);
            var content = (await _blobClient.DownloadContentAsync()).Value.Content.ToMemory();
            var blockList = (await blockListTask).Value.CommittedBlocks;
            var bookmarkBlockBuilder = ImmutableArray<BookmarkBlock>.Empty.ToBuilder();
            var blockIdBuilder = ImmutableArray<int>.Empty.ToBuilder();
            var offset = 0;

            foreach (var block in blockList)
            {
                var id = DecodeId(block.Name);
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
                    bookmarkBlockBuilder.Add(bookmarkBlock);
                }
                blockIdBuilder.Add(id);
                offset += block.Size;
            }
            _blockIds = blockIdBuilder.ToImmutableArray();
            _nextBlockId = _blockIds.Any() ? _blockIds.Max() + 1 : 0;

            return bookmarkBlockBuilder.ToImmutable();
        }

        public async Task<BookmarkTransactionResult> ApplyTransactionAsync(BookmarkTransaction transaction)
        {
            if (_blockIds == null)
            {
                throw new InvalidOperationException("Bookmark should have been read at this point");
            }
            var headerCount = _blockIds.Any() ? 0 : 1;
            var newBlockCount = (_blockIds.Any() ? 0 : 1) + transaction.AddingBlockBuffers.Count;
            var startBlockId = Interlocked.Add(ref _nextBlockId, newBlockCount);
            var headerBlockIds = Enumerable.Range(startBlockId, headerCount);
            var addingBlockIds = Enumerable.Range(
                startBlockId + headerCount,
                transaction.AddingBlockBuffers.Count);
            var updatingBlockIds = Enumerable.Range(
                startBlockId + headerCount + transaction.AddingBlockBuffers.Count,
                transaction.UpdatingBlocks.Count);
            var headerTasks = headerBlockIds
                .Select(id => _blobClient.StageBlockAsync(EncodeId(id), GetBookmarkHeaderStream()));
            var addingTasks = addingBlockIds
                .Zip(transaction.AddingBlockBuffers)
                .Select(pair => _blobClient.StageBlockAsync(
                    EncodeId(pair.First), new MemoryStream(pair.Second.ToArray())));
            var updatingTasks = updatingBlockIds
                .Zip(transaction.UpdatingBlocks)
                .Select(pair => _blobClient.StageBlockAsync(
                    EncodeId(pair.First), new MemoryStream(pair.Second.Buffer.ToArray())));
            var blockTasks = headerTasks.Concat(addingTasks).Concat(updatingTasks).ToArray();

            await Task.WhenAll(blockTasks);
            await CommitTransactionAsync(
                headerBlockIds.Concat(addingBlockIds).Concat(updatingBlockIds),
                transaction.DeletingBlockIds.Concat(transaction.UpdatingBlocks.Select(b => b.Id)));

            //  Do not put headerBlockIds on purpose as this is implementation detail for this class
            return new BookmarkTransactionResult(
                addingBlockIds,
                updatingBlockIds,
                transaction.DeletingBlockIds);
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

        private async Task CommitTransactionAsync(
            IEnumerable<int> blockIdsToAdd,
            IEnumerable<int> blockIdsToRemove)
        {   //  A bit of multithreading synchronization here
            var newItem = new CommitItem(blockIdsToAdd, blockIdsToRemove);

            //  Stack the item
            _commitItems.Push(newItem);

            //  Wait for the 'last' commiting task to be over
            await _commitingTask;

            //  Loop until somebody commit the item
            while (true)
            {
                if (newItem.Task.IsCompleted)
                {   //  Our item has been commited:  we're out
                    return;
                }
                //  Compete to be the next commiting task
                if (Monitor.TryEnter(_commitItems))
                {   //  This thread won and since it just queued the current item, it will process it
                    try
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
                            _commitingTask = CommitItemsAsync(items);
                            await _commitingTask;
                        }
                    }
                    finally
                    {
                        Monitor.Exit(_commitItems);
                    }
                }
                else
                {   //  Some other thread won, let's wait for it to be over and then restart the loop
                    await Task.WhenAny(_commitingTask, newItem.Task);
                    //  It is possible at this point that the new _commitingTask hasn't been
                    //  assigned, so we might loop a few times before it does
                }
            }
        }

        private async Task CommitItemsAsync(IEnumerable<CommitItem> items)
        {
            var blockIdsToAdd = items
                .Select(i => i.BlockIdsToAdd)
                .SelectMany(i => i);
            var blockIdsToRemove = items
                .Select(i => i.BlockIdsToRemove)
                .SelectMany(i => i);

            if (blockIdsToRemove.Any())
            {
                //  Make block id faster to manipulate
                var blockIdSet = _blockIds!.ToHashSet();

                foreach (var id in blockIdsToRemove)
                {
                    var success = blockIdSet.Remove(id);

                    if (!success)
                    {
                        throw new InvalidOperationException($"Block ID '{id}' couldn't be found and removed");
                    }
                }

                _blockIds = blockIdSet.Concat(blockIdsToAdd).OrderBy(id => id).ToImmutableArray();
            }
            else
            {
                _blockIds = _blockIds!.Concat(blockIdsToAdd).OrderBy(id => id).ToImmutableArray();
            }
            var newBlockIds = _blockIds
                .Select(id => EncodeId(id));

            //  Actually commit the new block list to the blob
            await _blobClient.CommitBlockListAsync(newBlockIds);

            //  Release all threads waiting for the items
            foreach (var item in items)
            {
                item.CompleteItem();
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