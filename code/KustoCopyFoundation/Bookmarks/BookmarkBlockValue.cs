using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyFoundation.Bookmarks
{
    public class BookmarkBlockValue<T>
    {
        public BookmarkBlockValue(int blockId, T value)
        {
            BlockId = blockId;
            Value = value;
        }

        public int BlockId { get; }

        public T Value { get; }

        public BookmarkBlockValue<V> Project<V>(Func<T, V> projection)
        {
            return new BookmarkBlockValue<V>(BlockId, projection(Value));
        }
    }
}