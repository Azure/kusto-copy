using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Unicode;
using System.Threading.Tasks;

namespace KustoCopyFoundation
{
    public static class SerializationHelper
    {
        private readonly static JsonSerializerOptions _options = new JsonSerializerOptions
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = true,
        };

        public static ReadOnlyMemory<byte> ToMemory<T>(T obj)
        {
            return new ReadOnlyMemory<byte>(ToBytes(obj));
        }

        public static byte[] ToBytes<T>(T obj)
        {
            var originalJson = JsonSerializer.Serialize(obj, _options);
            var spacedJson = originalJson + "\r\n";
            var buffer = UTF8Encoding.UTF8.GetBytes(spacedJson);

            return buffer;
        }

        public static T ToObject<T>(ReadOnlyMemory<byte> buffer)
        {
            var obj = JsonSerializer.Deserialize<T>(buffer.Span);

            if (obj == null)
            {
                throw new InvalidOperationException($"Couldn't deserialize object of type '{typeof(T).Name}'");
            }

            return obj;
        }
    }
}