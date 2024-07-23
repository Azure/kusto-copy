using CsvHelper.Configuration;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using CsvHelper.TypeConversion;
using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json.Serialization;
using System.Text.Json;

namespace KustoCopyConsole.Entity
{
    internal class RowItem
    {
        #region Inner types
        private class RowTypeConverter : DefaultTypeConverter
        {
            public override object? ConvertFromString(
                string? text,
                IReaderRow row,
                MemberMapData memberMapData)
            {
                if (string.IsNullOrWhiteSpace(text))
                {
                    return RowType.Unspecified;
                }
                else
                {
                    try
                    {
                        var state = RowType.Parse<RowType>(text);

                        return state;
                    }
                    catch (Exception ex)
                    {
                        throw new CopyException($"Can't deserialize row type:  '{text}'", false, ex);
                    }
                }
            }

            public override string? ConvertToString(
                object? value,
                IWriterRow row,
                MemberMapData memberMapData)
            {
                if (value != null)
                {
                    var text = value.ToString();

                    return text;
                }
                else
                {
                    return string.Empty;
                }
            }
        }
        #endregion

        #region Data
        [Index(0)]
        public string FileVersion { get; set; } = string.Empty;

        [Index(1)]
        public DateTime? Created { get; set; } = DateTime.Now;

        [Index(2)]
        public DateTime? Updated { get; set; } = DateTime.Now;

        [Index(3)]
        [TypeConverter(typeof(RowTypeConverter))]
        public RowType RowType { get; set; } = RowType.Unspecified;

        [Index(4)]
        public string State { get; set; } = string.Empty;

        [Index(5)]
        public string SourceClusterUri { get; set; } = string.Empty;

        [Index(6)]
        public string SourceDatabaseName { get; set; } = string.Empty;

        [Index(7)]
        public string DestinationClusterUri { get; set; } = string.Empty;

        [Index(8)]
        public string DestinationDatabaseName { get; set; } = string.Empty;

        [Index(9)]
        public string TableName { get; set; } = string.Empty;

        /// <summary>Zero-based index with zero being the backfill.</summary>>
        [Index(10)]
        public long? IterationId { get; set; }

        [Index(11)]
        public string CursorStart { get; set; } = string.Empty;

        [Index(12)]
        public string CursorEnd { get; set; } = string.Empty;

        [Index(13)]
        public long? BlockId { get; set; }
        #endregion

        public T ParseState<T>() where T : struct
        {
            try
            {
                return Enum.Parse<T>(State);
            }
            catch (Exception ex)
            {
                throw new CopyException($"Invalid state:  '{State}'", false, ex);
            }
        }

        public void Validate()
        {
            if (RowType == RowType.Unspecified)
            {
                throw new CopyException("Invalid row type", false);
            }
            ValidateProperty(
                nameof(FileVersion),
                string.IsNullOrWhiteSpace(FileVersion),
                RowType != RowType.FileVersion);
            ValidateProperty(
                nameof(Created),
                Created == null,
                false);
            ValidateProperty(
                nameof(Updated),
                Created == null,
                false);
            ValidateProperty(
                nameof(State),
                string.IsNullOrWhiteSpace(State),
                RowType == RowType.FileVersion);
        }

        public RowItem Clone()
        {
            var clone = (RowItem)MemberwiseClone();

            clone.Updated = DateTime.Now;

            return clone;
        }

        private void ValidateProperty(
            string propertyName,
            bool isNull,
            bool shouldBeNull)
        {
            if (isNull && !shouldBeNull)
            {
                throw new CopyException(
                    $"{propertyName} doesn't have value but should",
                    false);
            }
            if (!isNull && shouldBeNull)
            {
                throw new CopyException(
                    $"{propertyName} has value but should not",
                    false);
            }
        }
    }
}