﻿using CsvHelper.Configuration;
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
using KustoCopyConsole.Orchestrations;

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
            $"{nameof(IterationId)}:long, {nameof(EndCursor)}:string, "
            + $"{nameof(SubIterationId)}:long, {nameof(SubIterationEndTime)}:datetime, "
            + $"{nameof(RecordBatchId)}:long, {nameof(TableName)}:string, "
            + $"{nameof(State)}:string, "
            + $"{nameof(Created)}:datetime, {nameof(Updated)}:datetime, "
            + $"{nameof(InternalState)}:dynamic";

        #region Inner types
        private class InternalStateConverter : DefaultTypeConverter
        {
            public override object? ConvertFromString(
                string? text,
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
                object? value,
                IWriterRow row,
                MemberMapData memberMapData)
            {
                var state = (InternalState?)value;

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
        public static StatusItem CreateIteration(
            long iterationId,
            string endCursor)
        {
            var now = DateTime.UtcNow;
            var item = new StatusItem
            {
                IterationId = iterationId,
                EndCursor = endCursor,
                State = StatusItemState.Initial,
                Created = now,
                Updated = now
            };

            return item;
        }

        public static StatusItem CreateSubIteration(
            long iterationId,
            long subIterationId,
            DateTime? subIterationEndTime)
        {
            var now = DateTime.UtcNow;
            var item = new StatusItem
            {
                IterationId = iterationId,
                SubIterationId = subIterationId,
                SubIterationEndTime = subIterationEndTime,
                State = StatusItemState.Planned,
                Created = now,
                Updated = now,
                InternalState = new InternalState
                {
                    SubIterationState = new SubIterationState
                    {
                        StagingTableSuffix = Guid.NewGuid().ToString("N")
                    },
                }
            };

            return item;
        }

        public static StatusItem CreateRecordBatch(
            long iterationId,
            long subIterationId,
            long recordBatchId,
            string tableName,
            IEnumerable<TimeInterval> ingestionTimes,
            DateTime creationTime,
            long? recordCount)
        {
            var now = DateTime.UtcNow;
            var item = new StatusItem
            {
                IterationId = iterationId,
                SubIterationId = subIterationId,
                RecordBatchId = recordBatchId,
                TableName = tableName,
                State = StatusItemState.Planned,
                Created = now,
                Updated = now,
                InternalState = new InternalState
                {
                    RecordBatchState = new RecordBatchState
                    {
                        PlanRecordBatchState = new PlanRecordBatchState
                        {
                            IngestionTimes = ingestionTimes.ToImmutableArray(),
                            CreationTime = creationTime,
                            RecordCount = recordCount
                        }
                    }
                }
            };

            return item;
        }

        public StatusItem()
        {
        }
        #endregion

        [Ignore]
        public HierarchyLevel Level => RecordBatchId != null
            ? HierarchyLevel.RecordBatch
            : SubIterationId != null
            ? HierarchyLevel.SubIteration
            : HierarchyLevel.Iteration;

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
        public long? SubIterationId { get; set; }

        /// <summary>End time / Maximum ingestion time for the sub iteration.</summary>
        [Index(3)]
        public DateTime? SubIterationEndTime { get; set; }
        #endregion

        #region Record Batch
        /// <summary>Identifier of the record batch.</summary>
        [Index(4)]
        public long? RecordBatchId { get; set; }

        /// <summary>Table Name.</summary>
        [Index(5)]
        public string TableName { get; set; } = string.Empty;
        #endregion

        /// <summary>State of the item.</summary>
        [Index(6)]
        public StatusItemState State { get; set; } = StatusItemState.Initial;

        [Index(7)]
        public DateTime Created { get; set; }

        [Index(8)]
        public DateTime Updated { get; set; }

        [Index(9)]
        [TypeConverter(typeof(InternalStateConverter))]
        public InternalState InternalState { get; set; } = new InternalState();
        #endregion

        public string GetStagingTableName(StatusItem subIteration)
        {
            var suffix = subIteration.InternalState!.SubIterationState!.StagingTableSuffix;
            var stagingTableName = $"KC_STG_{TableName}_{suffix}";

            return stagingTableName;
        }

        public StatusItem UpdateState(StatusItemState applied)
        {
            var clone = Clone(clone =>
            {
                clone.State = applied;
                clone.Updated = DateTime.UtcNow;
            });

            return clone;
        }

        public StatusItem Clone(Action<StatusItem>? action = null)
        {
            var clone = (StatusItem)MemberwiseClone();

            if (action != null)
            {
                action(clone);
            }

            return clone;
        }
    }

    [JsonSerializable(typeof(InternalState))]
    internal partial class StatusItemSerializerContext : JsonSerializerContext
    {
    }
}