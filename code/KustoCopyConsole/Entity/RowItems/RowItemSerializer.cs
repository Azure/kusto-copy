using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class RowItemSerializer
    {
        private static readonly JsonSerializerOptions _serializerOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        private readonly IImmutableDictionary<RowType, Type> _rowTypeIndex;
        private readonly IImmutableDictionary<Type, RowType> _typeIndex;

        #region Constructors
        public RowItemSerializer()
        {
            _rowTypeIndex = ImmutableDictionary<RowType, Type>.Empty;
            _typeIndex = ImmutableDictionary<Type, RowType>.Empty;
        }

        private RowItemSerializer(
            IImmutableDictionary<RowType, Type> rowTypeIndex,
            IImmutableDictionary<Type, RowType> typeIndex)
        {
            _rowTypeIndex = rowTypeIndex;
            _typeIndex = typeIndex;
        }
        #endregion

        public RowItemSerializer AddType<T>(RowType rowType)
            where T : RowItemBase
        {
            var type = typeof(T);

            return new RowItemSerializer(
                _rowTypeIndex.Add(rowType, type),
                _typeIndex.Add(type, rowType));
        }

        #region Serialize
        public void Serialize(RowItemBase item, Stream stream)
        {
            if (_typeIndex.TryGetValue(item.GetType(), out var rowType))
            {
                using (var writer = new StreamWriter(stream, leaveOpen: true))
                {
                    writer.Write(@$"{{ ""rowType"" : ""{rowType}"", ""row"" : ");
                    writer.Flush();
                    JsonSerializer.Serialize(stream, item, item.GetType(), _serializerOptions);
                    writer.WriteLine(" }");
                }
            }
            else
            {
                throw new NotSupportedException(
                    $"Row type {item.GetType().Name} isn't supported");
            }
        }
        #endregion

        #region Deserialize
        public IImmutableList<RowItemBase> Deserialize(ReadOnlySpan<byte> buffer)
        {
            var builder = ImmutableList<RowItemBase>.Empty.ToBuilder();
            var remainingBuffer = buffer;

            while (!IsWhitespaceOnly(remainingBuffer))
            {
                var reader = new Utf8JsonReader(remainingBuffer, true, default);
                var rowType = ReadRowType(ref reader);
                var rowItemType = _rowTypeIndex[rowType];
                var row = ReadRow(ref reader, rowItemType);

                ReadEndObject(ref reader);
                builder.Add(row);
                remainingBuffer = remainingBuffer.Slice((int)reader.BytesConsumed);
            }

            return builder.ToImmutableArray();
        }

        private void ReadEndObject(ref Utf8JsonReader reader)
        {
            reader.Read();
            if (reader.TokenType == JsonTokenType.EndObject)
            {
            }
            else
            {
                throw new InvalidDataException(
                    $"Expect end-of-object, not '{reader.TokenType}'");
            }
        }

        private static RowItemBase ReadRow(ref Utf8JsonReader reader, Type rowItemType)
        {
            reader.Read();
            if (reader.TokenType == JsonTokenType.PropertyName)
            {
                var propertyName = reader.GetString();

                reader.Read(); // Move to property value
                if (propertyName == "row")
                {
                    var rowJson = JsonDocument.ParseValue(ref reader).RootElement.GetRawText();
                    var row = (RowItemBase?)JsonSerializer.Deserialize(
                        rowJson,
                        rowItemType,
                        _serializerOptions);

                    if (row == null)
                    {
                        throw new InvalidDataException(
                            $"Expected valid row object instead of '{rowJson}'");
                    }

                    return row;
                }
                else
                {
                    throw new InvalidDataException(
                        $"Expect 'row' JSON Property, not '{propertyName}'");
                }
            }
            else
            {
                throw new InvalidDataException("Expect JSON Property for 'row'");
            }
        }

        private static RowType ReadRowType(ref Utf8JsonReader reader)
        {
            reader.Read();
            if (reader.TokenType == JsonTokenType.StartObject)
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    var propertyName = reader.GetString();

                    reader.Read(); // Move to property value
                    if (propertyName == "rowType")
                    {
                        var rowTypeText = reader.GetString();

                        if (rowTypeText == null)
                        {
                            throw new InvalidDataException("Expect 'rowType' JSON Property to be non-null");
                        }

                        var rowType = Enum.Parse<RowType>(rowTypeText);

                        return rowType;
                    }
                    else
                    {
                        throw new InvalidDataException(
                            $"Expect 'rowType' JSON Property, not '{propertyName}'");
                    }
                }
                else
                {
                    throw new InvalidDataException("Expect JSON Property for 'rowType'");
                }
            }
            else
            {
                throw new InvalidDataException("Expect start JSON object");
            }
        }

        private static bool IsWhitespaceOnly(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length == 0)
            {
                return true;
            }
            else
            {
                foreach (var b in buffer)
                {
                    if (!char.IsWhiteSpace((char)b))
                    {
                        return false;
                    }
                }

                return true;
            }
        }
        #endregion
    }
}