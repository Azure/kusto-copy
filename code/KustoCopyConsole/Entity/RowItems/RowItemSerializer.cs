﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class RowItemSerializer
    {
        private readonly IImmutableDictionary<RowType, Func<RowItemBase>> _factoryIndex;
        private readonly IImmutableDictionary<Type, RowType> _typeIndex;
        private readonly IImmutableDictionary<Type, IImmutableList<PropertyInfo>>
            _typeToPropertyMap;

        #region Constructors
        public RowItemSerializer()
        {
            _factoryIndex = ImmutableDictionary<RowType, Func<RowItemBase>>.Empty;
            _typeIndex = ImmutableDictionary<Type, RowType>.Empty;
            _typeToPropertyMap = ImmutableDictionary<Type, IImmutableList<PropertyInfo>>.Empty;
        }

        private RowItemSerializer(
            IImmutableDictionary<RowType, Func<RowItemBase>> factoryIndex,
            IImmutableDictionary<Type, RowType> typeIndex,
            IImmutableDictionary<Type, IImmutableList<PropertyInfo>> typeToPropertyMap)
        {
            _factoryIndex = factoryIndex;
            _typeIndex = typeIndex;
            _typeToPropertyMap = typeToPropertyMap;
        }
        #endregion

        public RowItemSerializer AddType(RowType rowType, Func<RowItemBase> factory)
        {
            var type = factory().GetType();
            var properties = OrderByParent(type, type.GetProperties())
                .ToImmutableArray();

            return new RowItemSerializer(
                _factoryIndex.Add(rowType, factory),
                _typeIndex.Add(type, rowType),
                _typeToPropertyMap.Add(type, properties));
        }

        #region Serialize
        public void Serialize(RowItemBase item, TextWriter writer)
        {
            if (_typeIndex.TryGetValue(item.GetType(), out var rowType))
            {
                var properties = _typeToPropertyMap[item.GetType()];

                WriteEnum(writer, rowType);

                foreach (var property in properties)
                {
                    var propertyValue = property.GetValue(item, null);

                    WriteSeparator(writer);
                    WriteValue(writer, propertyValue);
                }
            }
            else
            {
                throw new NotSupportedException(
                    $"Row type {item.GetType().Name} isn't supported");
            }
        }

        private void WriteValue(TextWriter writer, object? propertyValue)
        {
            switch (propertyValue)
            {
                case string text:
                    WriteString(writer, text);
                    break;
                case Enum enumValue:
                    WriteEnum(writer, enumValue);
                    break;
                case DateTime datetimeValue:
                    WriteString(writer, datetimeValue.ToString());
                    break;
                case Version versionValue:
                    WriteString(writer, versionValue.ToString());
                    break;

                default:
                    throw new NotSupportedException(
                        $"Type {propertyValue?.GetType().Name} isn't supported in serialization");
            }
        }

        private static void WriteSeparator(TextWriter writer)
        {
            writer.Write(", ");
        }

        private static void WriteString(TextWriter writer, string text)
        {
            writer.Write(text);
        }

        private static void WriteEnum<T>(TextWriter writer, T enumValue)
            where T : Enum
        {
            WriteString(writer, enumValue.ToString());
        }
        #endregion

        public RowItemBase? Deserialize(TextReader reader)
        {
            throw new NotImplementedException();
        }

        private static IEnumerable<PropertyInfo> OrderByParent(
            Type parentType,
            IEnumerable<PropertyInfo> propertyInfos)
        {
            var directProperties = propertyInfos
                .Where(p => p.DeclaringType == parentType)
                .OrderBy(p => p.Name);
            var indirectProperties = propertyInfos
                .Where(p => p.DeclaringType != parentType);
            var baseType = parentType.BaseType!;

            if (baseType == typeof(object))
            {
                return directProperties;
            }
            else
            {
                return OrderByParent(baseType, indirectProperties)
                    .Concat(directProperties);
            }
        }
    }
}