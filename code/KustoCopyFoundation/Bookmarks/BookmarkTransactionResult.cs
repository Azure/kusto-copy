using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyFoundation.Bookmarks
{
    public class BookmarkTransactionResult
    {
        public BookmarkTransactionResult(
            IEnumerable<int> addedBlockIds,
            IEnumerable<int> updatedBlockIds,
            IEnumerable<int> deletedBlockIds)
        {
            AddedBlockIds = addedBlockIds.ToImmutableArray();
            UpdatedBlockIds = updatedBlockIds.ToImmutableArray();
            DeletedBlockIds = deletedBlockIds.ToImmutableArray();
        }

        public IImmutableList<int> AddedBlockIds { get; }
        
        public IImmutableList<int> UpdatedBlockIds { get; }

        public IImmutableList<int> DeletedBlockIds { get; }
    }
}