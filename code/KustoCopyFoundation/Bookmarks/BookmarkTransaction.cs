using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyFoundation.Bookmarks
{
    public class BookmarkTransaction
    {
        public BookmarkTransaction(
            IEnumerable<ReadOnlyMemory<byte>>? addingBlockBuffers,
            IEnumerable<BookmarkBlock>? updatingBlocks,
            IEnumerable<int>? deletingBlockIds)
        {
            AddingBlockBuffers = addingBlockBuffers == null
                ? ImmutableArray<ReadOnlyMemory<byte>>.Empty
                : addingBlockBuffers.ToImmutableArray();
            UpdatingBlocks = updatingBlocks == null
                ? ImmutableArray<BookmarkBlock>.Empty
                : updatingBlocks.ToImmutableArray();
            DeletingBlockIds = deletingBlockIds == null
                ? ImmutableArray<int>.Empty
                : deletingBlockIds.ToImmutableArray();
        }

        public IImmutableList<ReadOnlyMemory<byte>> AddingBlockBuffers { get; }

        public IImmutableList<BookmarkBlock> UpdatingBlocks { get; }

        public IImmutableList<int> DeletingBlockIds { get; }
    }
}