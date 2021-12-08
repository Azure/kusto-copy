namespace KustoCopyBookmarks
{
    public class BookmarkBlock
    {
        public BookmarkBlock(int id, ReadOnlyMemory<byte> buffer)
        {
            Id = id;
            Buffer = buffer;
        }

        public int Id { get; }

        public ReadOnlyMemory<byte> Buffer { get; }
    }
}