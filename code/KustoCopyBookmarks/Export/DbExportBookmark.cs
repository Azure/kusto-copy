using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Parameters;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Text.Json;

namespace KustoCopyBookmarks.Export
{
    public class DbExportBookmark
    {
        #region Inner Types
        private class ExportAggregate
        {
            public DbEpochData? DbEpoch { get; set; }

            public DbIterationData? DbIteration { get; set; }

            public TableIterationData? TableIteration { get; set; }

            public EmptyTableExportEventData? EmptyTableExportEvent { get; set; }

            public TableExportEventData? TableExportEvent { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly List<BookmarkBlockValue<DbEpochData>> _dbEpochs;
        private readonly List<BookmarkBlockValue<DbIterationData>> _dbIterations;
        //private ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>
        //    _backfillTableIterationMap;
        //private ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>
        //    _forwardTableIterationMap;
        //private ConcurrentQueue<BookmarkBlockValue<EmptyTableExportEventData>> _emptyTableExportEvents;

        public static async Task<DbExportBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = new BookmarkGateway(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<ExportAggregate>();
            var epochs = aggregates
                .Where(a => a.Value.DbEpoch != null);
            var dbIterations = aggregates
                .Where(a => a.Value.DbIteration != null);
            var tableIterations = aggregates
                .Where(a => a.Value.TableIteration != null);
            var emptyTableExportEvents = aggregates
                .Where(a => a.Value.EmptyTableExportEvent != null);

            if (epochs.Count() > 2)
            {
                throw new InvalidOperationException(
                    "Expected at most two iterations definition block");
            }

            return new DbExportBookmark(
                bookmarkGateway,
                epochs.Select(b => b.Project(a => a.DbEpoch!)),
                dbIterations.Select(b => b.Project(a => a.DbIteration!)),
                emptyTableExportEvents.Select(e => e.Project(e => e.EmptyTableExportEvent!)));
        }

        //public async Task<IImmutableList<string>> ProcessEmptyTableAsync(
        //    bool isBackfill,
        //    Func<IEnumerable<string>, Task<IEnumerable<TableSchemaData>>> fetchSchemasAsync)
        //{
        //    var map = isBackfill ? _backfillTableIterationMap : _forwardTableIterationMap;
        //    var dbIteration = isBackfill
        //        ? _backfillDbEpoch!.Value
        //        : _backfillDbEpoch!.Value;
        //    var emptyTableNames = map
        //        .Values
        //        .Select(t => t.Value)
        //        .Where(t => t.MinRemainingIngestionTime == null)
        //        .Select(t => t.TableName)
        //        .ToImmutableArray();
        //    var emptyTableIds = emptyTableNames
        //        .Select(n => map[n].BlockId);
        //    var schemas = await fetchSchemasAsync(emptyTableNames);
        //    var events = schemas
        //        .Zip(emptyTableNames, (s, n) => new { Schema = s, TableName = n })
        //        .Select(p => new EmptyTableExportEventData
        //        {
        //            EpochEndCursor = dbIteration.EndCursor,
        //            TableName = p.TableName,
        //            Schema = p.Schema
        //        });
        //    var eventBuffers = events
        //        .Select(e => SerializationHelper.ToMemory(
        //            new ExportAggregate { EmptyTableExportEvent = e }));
        //    var transaction = new BookmarkTransaction(eventBuffers, null, emptyTableIds);
        //    //  Persist to blob
        //    var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);
        //    var eventValues = events
        //        .Zip(
        //        result.AddedBlockIds,
        //        (e, id) => new BookmarkBlockValue<EmptyTableExportEventData>(id, e));

        //    //  Persist to memory
        //    foreach (var tableName in emptyTableNames)
        //    {
        //        if (!map.TryRemove(tableName, out _))
        //        {
        //            throw new InvalidOperationException(
        //                $"Can't remove table '{tableName}' in memory with backfill={isBackfill}");
        //        }
        //    }
        //    foreach (var eventValue in eventValues)
        //    {
        //        _emptyTableExportEvents.Enqueue(eventValue);
        //    }

        //    return emptyTableNames;
        //}

        //public IImmutableList<string> GetNextDayTables(bool isBackfill)
        //{
        //    if (isBackfill)
        //    {
        //        var nextDay = _backfillTableIterationMap.Values
        //            .Select(i => i.Value.MaxRemainingIngestionTime)
        //            .Where(d => d != null)
        //            .Cast<DateTime>()
        //            .Aggregate(
        //            (DateTime?)null,
        //            (dMax, d) => dMax == null ? d : (d > dMax ? d : dMax));

        //        if (nextDay == null)
        //        {
        //            return ImmutableArray<string>.Empty;
        //        }
        //        else
        //        {
        //            var nextDayDate = nextDay.Value.Date;
        //            var tableNames = _backfillTableIterationMap.Values
        //                .Select(b => b.Value)
        //                .Where(i => i.MaxRemainingIngestionTime != null
        //                && i.MaxRemainingIngestionTime.Value.Date == nextDayDate)
        //                .Select(i => i.TableName);

        //            return tableNames.ToImmutableArray();
        //        }
        //    }
        //    else
        //    {
        //        throw new NotImplementedException();
        //    }
        //}

        private DbExportBookmark(
            BookmarkGateway bookmarkGateway,
            IEnumerable<BookmarkBlockValue<DbEpochData>> dbEpochs,
            IEnumerable<BookmarkBlockValue<DbIterationData>> dbIterations,
            IEnumerable<BookmarkBlockValue<EmptyTableExportEventData>> emptyTableExportEvents)
        {
            _bookmarkGateway = bookmarkGateway;
            _dbEpochs = new List<BookmarkBlockValue<DbEpochData>>(dbEpochs);
            _dbIterations = new List<BookmarkBlockValue<DbIterationData>>(dbIterations);

            //var backfillTableIterations = tableIterations
            //    .Where(t => t.Value.EpochEndCursor == _backfillDbEpoch?.Value.EndCursor)
            //    .Select(t => KeyValuePair.Create(t.Value.TableName, t));
            //var forwardTableIterations = tableIterations
            //    .Where(t => t.Value.EpochEndCursor == _forwardDbEpoch?.Value.EndCursor)
            //    .Select(t => KeyValuePair.Create(t.Value.TableName, t));

            //_backfillTableIterationMap = new ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>(backfillTableIterations);
            //_forwardTableIterationMap = new ConcurrentDictionary<string, BookmarkBlockValue<TableIterationData>>(forwardTableIterations);
            //_emptyTableExportEvents = new ConcurrentQueue<BookmarkBlockValue<EmptyTableExportEventData>>(emptyTableExportEvents);
        }

        public DbEpochData? GetDbEpoch(bool isBackfill)
        {
            var value = GetDbEpochValue(isBackfill);

            return value == null
                ? null
                : value.Value;
        }

        public async Task<DbEpochData> CreateNewEpochAsync(
            bool isBackfill,
            DateTime currentTime,
            string cursor)
        {
            var existingEpoch = GetDbEpochValue(isBackfill);
            var newEpoch = new DbEpochData
            {
                EndCursor = cursor,
                EpochStartTime = currentTime
            };

            if (existingEpoch != null && !existingEpoch.Value.AllIterationsExported)
            {
                throw new InvalidOperationException("There already is an existing epoch");
            }
            if (existingEpoch != null && isBackfill)
            {
                throw new InvalidOperationException("There can be only one backfill epoch");
            }
            if (existingEpoch == null && !isBackfill)
            {
                var existingBackfillEpoch = GetDbEpochValue(true);

                if (existingBackfillEpoch == null)
                {
                    throw new InvalidOperationException(
                        "We can't have a forward epoch before a backfill epoch has been initiated");
                }
                else
                {
                    newEpoch.StartCursor = existingBackfillEpoch.Value.EndCursor;
                }
            }
            var eventBuffer =
                SerializationHelper.ToMemory(new ExportAggregate { DbEpoch = newEpoch });
            var transaction = new BookmarkTransaction(
                new[] { eventBuffer },
                null,
                existingEpoch != null ? new[] { existingEpoch.BlockId } : ImmutableArray<int>.Empty);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);

            lock (_dbEpochs)
            {
                if (existingEpoch != null)
                {
                    _dbEpochs.Remove(existingEpoch);
                }
                _dbEpochs.Add(new BookmarkBlockValue<DbEpochData>(
                    result.AddedBlockIds.First(),
                    newEpoch));

                return newEpoch;
            }
        }

        public IImmutableList<DbIterationData> GetDbIterations(string endCursor)
        {
            var iterations = _dbIterations
                .Select(i => i.Value)
                .Where(i => i.EpochEndCursor == endCursor)
                .ToImmutableArray();

            return iterations;
        }

        private BookmarkBlockValue<DbEpochData>? GetDbEpochValue(bool isBackfill)
        {
            lock (_dbEpochs)
            {
                return _dbEpochs
                    .Where(e => e.Value.IsBackfill == isBackfill)
                    .FirstOrDefault();
            }
        }
    }
}