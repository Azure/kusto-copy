using CsvHelper.Configuration;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using CsvHelper.TypeConversion;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class StatusItem
    {
        private readonly static StatusItemSerializerContext _statusItemSerializerContext =
            new StatusItemSerializerContext(
                new JsonSerializerOptions
                {
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                    WriteIndented = false
                });

        public static string ExternalTableSchema =>
            $"{nameof(IterationId)}:long, {nameof(EndCursor)}:string, {nameof(SubIterationId)}:long, "
            + $"{nameof(StartIngestionTime)}:datetime, {nameof(EndIngestionTime)}:datetime, "
            + $"{nameof(TableName)}:string, {nameof(RecordBatchId)}:long, "
            + $"{nameof(State)}:string, {nameof(Timestamp)}:datetime, {nameof(InternalState)}:dynamic";

        #region Inner types
        private class InternalStateConverter : DefaultTypeConverter
        {
            public override object? ConvertFromString(
                string text,
                IReaderRow row,
                MemberMapData memberMapData)
            {
                if (string.IsNullOrWhiteSpace(text))
                {
                    return new InternalState();
                }
                else
                {
                    var state = JsonSerializer.Deserialize(
                        text,
                        typeof(InternalState),
                        _statusItemSerializerContext);

                    if (state == null)
                    {
                        throw new CopyException($"Can't deserialize internal state:  '{text}'");
                    }

                    return state;
                }
            }

            public override string? ConvertToString(
                object value,
                IWriterRow row,
                MemberMapData memberMapData)
            {
                var state = (InternalState)value;

                if (state != null)
                {
                    var text = JsonSerializer.Serialize(
                        state,
                        typeof(InternalState),
                        _statusItemSerializerContext);

                    return text;
                }
                else
                {
                    return string.Empty;
                }
            }
        }
        #endregion

        #region Constructors
        public static StatusItem CreateIteration(long iterationId, string endCursor)
        {
            var item = new StatusItem
            {
                IterationId = iterationId,
                EndCursor = endCursor,
                State = StatusItemState.Initial,
                Timestamp = DateTime.UtcNow
            };

            return item;
        }

        public static StatusItem CreateSubIteration(
            long iterationId,
            string endCursor,
            long subIterationId,
            DateTime? startIngestionTime,
            DateTime? endIngestionTime)
        {
            var item = new StatusItem
            {
                IterationId = iterationId,
                SubIterationId = subIterationId,
                EndCursor = endCursor,
                StartIngestionTime = startIngestionTime,
                EndIngestionTime = endIngestionTime,
                State = StatusItemState.Initial,
                Timestamp = DateTime.UtcNow
            };

            return item;
        }

        public StatusItem()
        {
        }
        #endregion

        [Ignore]
        public HierarchyLevel Level => HierarchyLevel.Iteration;

        #region Data properties
        #region Iteration
        /// <summary>Identifier of the iteration.</summary>
        [Index(0)]
        public long IterationId { get; set; }

        /// <summary>End cursor of the iteration.</summary>
        [Index(1)]
        public string EndCursor { get; set; } = string.Empty;
        #endregion

        #region Sub Iteration
        /// <summary>Identifier of the sub iteration.</summary>
        [Index(2)]
        public long SubIterationId { get; set; }

        /// <summary>Start ingestion time (inclusive) for the sub iteration.</summary>
        [Index(3)]
        public DateTime? StartIngestionTime { get; set; }

        /// <summary>End ingestion time (exclusive) for the sub iteration.</summary>
        [Index(4)]
        public DateTime? EndIngestionTime { get; set; }
        #endregion

        #region Table
        /// <summary>Table Name.</summary>
        [Index(5)]
        public string TableName { get; set; } = string.Empty;
        #endregion

        #region Record Batch
        /// <summary>Identifier of the record batch.</summary>
        [Index(6)]
        public long RecordBatchId { get; set; }
        #endregion

        /// <summary>State of the item.</summary>
        [Index(7)]
        public StatusItemState State { get; set; } = StatusItemState.Initial;

        [Index(8)]
        public DateTime Timestamp { get; set; }

        [Index(9)]
        [TypeConverter(typeof(InternalStateConverter))]
        public InternalState InternalState { get; set; } = new InternalState();
        #endregion
    }

    [JsonSerializable(typeof(InternalState))]
    internal partial class StatusItemSerializerContext : JsonSerializerContext
    {
    }
}