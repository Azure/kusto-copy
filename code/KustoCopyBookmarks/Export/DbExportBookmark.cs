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

            public TableIterationData? TableIteration { get; set; }

            public EmptyTableExportEventData? EmptyTableExportEvent { get; set; }

            public TableExportEventData? TableExportEvent { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly ConcurrentDictionary<string, BookmarkBlockValue<DbEpochData>>
            _endCursorToEpoch;
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
                tableIterations.Select(b => b.Project(a => a.TableIteration!)),
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
            IEnumerable<BookmarkBlockValue<TableIterationData>> tableIterations,
            IEnumerable<BookmarkBlockValue<EmptyTableExportEventData>> emptyTableExportEvents)
        {
            _bookmarkGateway = bookmarkGateway;
            _endCursorToEpoch = new ConcurrentDictionary<string, BookmarkBlockValue<DbEpochData>>(
                dbEpochs.Select(e => KeyValuePair.Create(e.Value.EndCursor, e)));

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
    }
}