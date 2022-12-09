using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestrations
{
    internal class TablePlanningOrchestration
    {
        #region Inner types
        public record PlanningOutput(
            bool HasMoreRecords,
            DateTime? IterationTableEndTime,
            IImmutableList<PlanRecordBatchState> RecordBatches);

        private record ProtoRecordBatch(
            TimeWindow IngestionTimeInterval,
            long RecordCount,
            string ExtentId);
        #endregion

        private readonly KustoPriority _kustoPriority;
        private readonly CursorWindow _cursorWindow;
        private readonly string _startTimePredicate;
        private readonly long _maxExtentCount;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructors
        public static async Task<PlanningOutput> PlanAsync(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? startTimeExclusive,
            long maxExtentCount,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TablePlanningOrchestration(
                kustoPriority,
                cursorWindow,
                startTimeExclusive,
                maxExtentCount,
                sourceQueuedClient);
            var batches = await orchestration.PlanAsync(ct);

            return batches;
        }

        public TablePlanningOrchestration(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? startTimeExclusive,
            long maxExtentCount,
            KustoQueuedClient sourceQueuedClient)
        {
            _kustoPriority = kustoPriority;
            _cursorWindow = cursorWindow;
            _startTimePredicate = startTimeExclusive == null
                ? string.Empty
                : $"| where ingestion_time() > {startTimeExclusive.Value.ToKql()}";
            _maxExtentCount = maxExtentCount;
            _sourceQueuedClient = sourceQueuedClient;
        }
        #endregion

        private async Task<PlanningOutput> PlanAsync(
            CancellationToken ct)
        {
            var iterationTableEndTimeTask = FetchIterationTableEndTimeAsync();

            return await PlanAsync(iterationTableEndTimeTask, ct);
        }

        private async Task<PlanningOutput> PlanAsync(
            Task<DateTime?> iterationTableEndTimeTask,
            CancellationToken ct)
        {
            var protoRecordBatches = await LoadProtoRecordBatchesAsync();

            if (protoRecordBatches.Any())
            {
                var extentIdList = protoRecordBatches
                    .Select(p => p.ExtentId)
                    .Distinct()
                    .ToImmutableArray();
                var extentMap = await FetchExtentIdMapAsync(extentIdList);

                if (extentMap.Count() != extentIdList.Count())
                {
                    Trace.TraceWarning(
                        $"Extent list changed between 2 close operations on "
                        + $"table '{_kustoPriority.TableName}':  "
                        + $"'{protoRecordBatches.Count()}' vs '{extentMap.Count()}'.  "
                        + "Likely cause:  merge.  Mitigation:  retry.");

                    return await PlanAsync(iterationTableEndTimeTask, ct);
                }
                else
                {
                    var recordBatches = MapRecordBatches(protoRecordBatches, extentMap);
                    var maxIngestionTime = recordBatches
                        .SelectMany(r => r.IngestionTimes)
                        .Max(i => i.EndTime);
                    var iterationTableEndTime = await iterationTableEndTimeTask;
                    var hasMoreRecords = iterationTableEndTime > maxIngestionTime;

                    return new PlanningOutput(
                        hasMoreRecords,
                        iterationTableEndTime,
                        recordBatches);
                }
            }
            else
            {
                var iterationMaxIngestionTime = await iterationTableEndTimeTask;

                return new PlanningOutput(
                    false,
                    iterationMaxIngestionTime,
                    ImmutableArray<PlanRecordBatchState>.Empty);
            }
        }

        private static ImmutableArray<PlanRecordBatchState> MapRecordBatches(
            IImmutableList<ProtoRecordBatch> protoRecordBatches,
            IImmutableDictionary<string, DateTime> extentMap)
        {
            var recordBatches = protoRecordBatches
                .GroupBy(p => p.ExtentId)
                .Select(g => new PlanRecordBatchState
                {
                    CreationTime = extentMap[g.Key],
                    RecordCount = g.Sum(p => p.RecordCount),
                    IngestionTimes = g.Select(p => new TimeInterval
                    {
                        StartTime = p.IngestionTimeInterval.StartTime,
                        EndTime = p.IngestionTimeInterval.EndTime
                    }).ToImmutableArray()
                })
                .ToImmutableArray();

            return recordBatches;
        }

        private async Task<DateTime?> FetchIterationTableEndTimeAsync()
        {
            var commandText = $@"['{_kustoPriority.TableName}']
{_cursorWindow.ToCursorKustoPredicate()}
| summarize MaxTime = max(ingestion_time())";
            var outputs = await _sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                commandText,
                r => r["MaxTime"].To<DateTime>());
            var output = outputs.First();

            return output;
        }

        private async Task<IImmutableList<ProtoRecordBatch>> LoadProtoRecordBatchesAsync()
        {
            var sourceQueuedClient = _sourceQueuedClient;
            var queryText = $@"
let MaxExtentCount = {_maxExtentCount};
let FramedTable = ['{_kustoPriority.TableName}']
    {_cursorWindow.ToCursorKustoPredicate()}
    {_startTimePredicate};
let Extremes = materialize(FramedTable
    | extend ExtentId=extent_id()
    | extend IngestionTime=ingestion_time()
    | summarize MinIngestionTime=min(ingestion_time()), MaxIngestionTime=max(ingestion_time()) by ExtentId
    | top MaxExtentCount by MinIngestionTime asc
    | where isnotnull(MinIngestionTime)
    | where isnotnull(MaxIngestionTime)
    | summarize MinIngestionTime=min(MinIngestionTime), MaxIngestionTime=max(MaxIngestionTime));
let MinIngestionTime = toscalar(Extremes | project MinIngestionTime);
let MaxIngestionTime = toscalar(Extremes | project MaxIngestionTime);
let MicroRecordBatches = FramedTable
    | where ingestion_time() >= MinIngestionTime
    | where ingestion_time() <= MaxIngestionTime
    | summarize Cardinality=count() by IngestionTime=ingestion_time(), ExtentId=extent_id();
MicroRecordBatches
| order by IngestionTime asc
| extend IsNewExtentId = iif(prev(ExtentId)==ExtentId, false, true)
| extend IsNextNewExtentId = iif(next(ExtentId)==ExtentId, false, true)
| extend TotalCardinality = row_cumsum(Cardinality, IsNewExtentId)
| where IsNewExtentId or IsNextNewExtentId
| extend MinIngestionTime=IngestionTime
| extend MaxIngestionTime=iif(not(IsNextNewExtentId), next(IngestionTime), IngestionTime)
| extend ActualTotalCardinality = iif(IsNextNewExtentId, TotalCardinality, next(TotalCardinality))
| where IsNewExtentId
| project MinIngestionTime, MaxIngestionTime, tostring(ExtentId), Cardinality=ActualTotalCardinality";
            var protoBatches = await sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                queryText,
                r => new ProtoRecordBatch(
                    new TimeWindow(
                        (DateTime)r["MinIngestionTime"],
                        (DateTime)r["MaxIngestionTime"]),
                    (long)r["Cardinality"],
                    (string)r["ExtentId"]));

            return protoBatches;
        }

        private async Task<IImmutableDictionary<string, DateTime>> FetchExtentIdMapAsync(
            IEnumerable<string> extentIds)
        {
            var extentIdTextList = string.Join(
                Environment.NewLine + ", ",
                extentIds.Select(e => $"'{e}'"));
            var commandText = $@".show table ['{_kustoPriority.TableName}'] extents
({extentIdTextList})
| project tostring(ExtentId), MaxCreatedOn
";
            var mapList = await _sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                commandText,
                r => new
                {
                    ExtentId = (string)r["ExtentId"],
                    MaxCreatedOn = (DateTime)r["MaxCreatedOn"]
                });
            var map = mapList.ToImmutableDictionary(p => p.ExtentId, p => p.MaxCreatedOn);

            return map;
        }
    }
}