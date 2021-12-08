using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks
{
    internal class BookmarkBlockValue<T>
    {
        public BookmarkBlockValue(int blockId, T value)
        {
            BlockId = blockId;
            Value = value;
        }

        public int BlockId { get; }

        public T Value { get; }
    }
}