using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace KustoCopyBookmarks
{
    internal static class SerializationHelper
    {
        public static ReadOnlyMemory<byte> SerializeToMemory<T>(T obj)
        {
            using (var stream = SerializeToStream(obj))
            {
                return new ReadOnlyMemory<byte>(stream.ToArray());
            }
        }

        public static MemoryStream SerializeToStream<T>(T obj)
        {
            var stream = new MemoryStream();

            JsonSerializer.Serialize(stream, obj);
            stream.Position = 0;

            return stream;
        }

        public static MemoryStream ToStream(ReadOnlyMemory<byte> buffer)
        {
            var stream = new MemoryStream(buffer.ToArray());

            return stream;
        }
    }
}