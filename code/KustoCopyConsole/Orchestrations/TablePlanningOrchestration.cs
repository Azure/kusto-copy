using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Linq;
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
            DateTime? IterationTableEndTime,
            IImmutableList<PlanRecordBatchState> RecordBatches);

        private record ProtoRecordBatch(
            TimeWindow IngestionTimeInterval,
            long RecordCount,
            string ExtentId);
        #endregion

        private const long MAX_MERGE_RECORD_COUNT = 100000;
        private static readonly TimeSpan MAX_MERGE_TIME_GAP = TimeSpan.FromHours(1);

        private readonly KustoPriority _kustoPriority;
        private readonly CursorWindow _cursorWindow;
        private readonly string _startTimePredicate;
        private readonly long _minExtentCount;
        private readonly long _maxExtentCount;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructors
        public static async Task<PlanningOutput> PlanAsync(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? startTimeExclusive,
            long minExtentCount,
            long maxExtentCount,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TablePlanningOrchestration(
                kustoPriority,
                cursorWindow,
                startTimeExclusive,
                minExtentCount,
                maxExtentCount,
                sourceQueuedClient);
            var batches = await orchestration.PlanAsync(ct);

            return batches;
        }

        public TablePlanningOrchestration(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? startTimeExclusive,
            long minExtentCount,
            long maxExtentCount,
            KustoQueuedClient sourceQueuedClient)
        {
            _kustoPriority = kustoPriority;
            _cursorWindow = cursorWindow;
            _startTimePredicate = startTimeExclusive == null
                ? string.Empty
                : $"| where ingestion_time() > {startTimeExclusive.Value.ToKql()}";
            _minExtentCount = minExtentCount;
            _maxExtentCount = maxExtentCount;
            _sourceQueuedClient = sourceQueuedClient;
        }
        #endregion

        private async Task<PlanningOutput> PlanAsync(
            CancellationToken ct)
        {
            var iterationTableEndTime = await FetchIterationTableEndTimeAsync();

            if (iterationTableEndTime == null)
            {
                return new PlanningOutput(
                    null,
                    ImmutableArray<PlanRecordBatchState>.Empty);
            }
            else
            {
                return await PlanWithInterpolationAsync(iterationTableEndTime.Value, ct);
            }
        }

        private async Task<PlanningOutput> PlanWithInterpolationAsync(
            DateTime iterationTableEndTime,
            CancellationToken ct)
        {
            PlanningOutput? bestOutput = null;
            var nextExtentCount = _maxExtentCount;

            while (true)
            {
                var output = await PlanAsync(iterationTableEndTime, nextExtentCount, ct);
                var mergedRecordBatches = PlanRecordBatchState.Merge(
                    output.RecordBatches,
                    MAX_MERGE_RECORD_COUNT,
                    MAX_MERGE_TIME_GAP);
                var mergedOutput =
                    new PlanningOutput(output.IterationTableEndTime, mergedRecordBatches);

                if (
                    //  There are no more extents to fetch from
                    output.RecordBatches.Count < nextExtentCount
                    //  We meet the target
                    || (mergedRecordBatches.Count() >= _minExtentCount
                    && mergedRecordBatches.Count() <= _maxExtentCount)
                    //  We overshoted on first try
                    || (mergedRecordBatches.Count() > _maxExtentCount
                    && bestOutput == null))
                {
                    return mergedOutput;
                }
                else if (mergedRecordBatches.Count() > _maxExtentCount && bestOutput != null)
                {   //  We overshoted, fall back on previous
                    return bestOutput;
                }
                else
                {
                    bestOutput = mergedOutput;
                    nextExtentCount = nextExtentCount * _maxExtentCount
                        / mergedOutput.RecordBatches.Count();
                }
            }
        }

        private async Task<PlanningOutput> PlanAsync(
            DateTime iterationTableEndTime,
            long extentCount,
            CancellationToken ct)
        {
            var protoRecordBatches = await LoadProtoRecordBatchesAsync(extentCount);

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

                    return await PlanAsync(iterationTableEndTime, extentCount, ct);
                }
                else
                {
                    var recordBatches = MapRecordBatches(protoRecordBatches, extentMap);

                    return new PlanningOutput(
                        iterationTableEndTime,
                        recordBatches);
                }
            }
            else
            {
                return new PlanningOutput(
                    iterationTableEndTime,
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
| summarize MaxTime=max(ingestion_time())";
            var maxima = await _sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                commandText,
                r => r["MaxTime"].To<DateTime>());
            var maximum = maxima.First();

            return maximum;
        }

        private async Task<IImmutableList<ProtoRecordBatch>> LoadProtoRecordBatchesAsync(
            long extentCount)
        {
            var sourceQueuedClient = _sourceQueuedClient;
            var queryText = $@"
let ExtentCount = {extentCount};
let FramedTable = ['{_kustoPriority.TableName}']
    {_cursorWindow.ToCursorKustoPredicate()}
    {_startTimePredicate};
let Extremes = materialize(FramedTable
    | extend ExtentId=extent_id()
    | extend IngestionTime=ingestion_time()
    | summarize MinIngestionTime=min(ingestion_time()), MaxIngestionTime=max(ingestion_time()) by ExtentId
    | top ExtentCount by MinIngestionTime asc
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