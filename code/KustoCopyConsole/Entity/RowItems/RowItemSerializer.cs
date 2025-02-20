using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class RowItemSerializer
    {
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
        public string Serialize(RowItemBase item)
        {
            if (_typeIndex.TryGetValue(item.GetType(), out var rowType))
            {
                var itemText = JsonSerializer.Serialize(
                    item,
                    RowItemJsonContext.Default.GetTypeInfo(item.GetType())!);
                var wrapperText = @$"{{ ""rowType"" : ""{rowType}"", ""row"" : {itemText} }}";

                return wrapperText + '\n';
            }
            else
            {
                throw new NotSupportedException(
                    $"Row type {item.GetType().Name} isn't supported");
            }
        }
        #endregion

        #region Deserialize
        public RowItemBase Deserialize(string text)
        {
            var document = JsonDocument.Parse(text);

            if (document.RootElement.TryGetProperty("rowType", out var rowTypeElement))
            {
                var rowTypeText = rowTypeElement.GetString()!;

                if (Enum.TryParse<RowType>(rowTypeText, out var rowType))
                {
                    var rowItemType = _rowTypeIndex[rowType];

                    if (document.RootElement.TryGetProperty("row", out var rowElement))
                    {
                        var rowElementText = rowElement.GetRawText();
                        var itemObject = JsonSerializer.Deserialize(
                            rowElementText,
                            RowItemJsonContext.Default.GetTypeInfo(rowItemType)!);

                        if (itemObject != null)
                        {
                            return (RowItemBase)itemObject;
                        }
                        else
                        {
                            throw new InvalidDataException(
                                $"Can't deserialize row:  {rowElement.GetRawText()}");
                        }
                    }
                    else
                    {
                        throw new InvalidDataException($"Expected a property 'rowType':  {text}");
                    }
                }
                else
                {
                    throw new InvalidDataException($"Unexpected 'rowType':  '{rowTypeText}'");
                }
            }
            else
            {
                throw new InvalidDataException($"Expect 'rowType' JSON Property:  {text}");
            }
        }
        #endregion
    }
}