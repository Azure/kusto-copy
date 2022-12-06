using Kusto.Data.Common;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Net.NetworkInformation;

namespace KustoCopyConsole.Orchestrations
{
    public class TablePlanningOrchestration
    {
        #region Inner types
        private record ProtoRecordBatch(
            TimeWindow IngestionTimeInterval,
            long RecordCount,
            string ExtentId);
        #endregion

        private const int RECORD_BATCH_SIZE = 10000;

        private readonly KustoPriority _kustoPriority;
        private readonly StatusItem _subIteration;
        private readonly string _cursorWindowPredicate;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _sourceQueuedClient;
        private readonly Func<long> _recordBatchIdProvider;

        #region Constructors
        public static async Task PlanAsync(
            KustoPriority kustoPriority,
            StatusItem subIteration,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            Func<long> recordBatchIdProvider,
            CancellationToken ct)
        {
            var orchestration = new TablePlanningOrchestration(
                kustoPriority,
                subIteration,
                dbStatus,
                sourceQueuedClient,
                recordBatchIdProvider);

            await orchestration.RunAsync(ct);
        }

        private TablePlanningOrchestration(
            KustoPriority kustoPriority,
            StatusItem subIteration,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            Func<long> recordBatchIdProvider)
        {
            _kustoPriority = kustoPriority;
            _subIteration = subIteration;
            _cursorWindowPredicate = dbStatus
                .GetCursorWindow(subIteration.IterationId)
                .ToCursorKustoPredicate();
            _dbStatus = dbStatus;
            _sourceQueuedClient = sourceQueuedClient;
            _recordBatchIdProvider = recordBatchIdProvider;
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            do
            {
                var batches = _dbStatus.GetRecordBatches(
                    _subIteration.IterationId,
                    _subIteration.SubIterationId!.Value)
                    .Where(b => b.TableName == _kustoPriority.TableName);
                //  Clip out the bracket considering existing batches
                var startTime = batches
                    .SelectMany(b =>
                    b.InternalState.RecordBatchState!.PlanRecordBatchState!.IngestionTimes.Select(t => t.EndTime))
                    .Append(_subIteration.InternalState.SubIterationState!.StartIngestionTime!.Value)
                    .Max();
                var endTime = batches
                    .SelectMany(b =>
                    b.InternalState.RecordBatchState!.PlanRecordBatchState!.IngestionTimes.Select(t => t.StartTime))
                    .Append(_subIteration.InternalState.SubIterationState!.EndIngestionTime!.Value)
                    .Min();
                var includeStartTime = startTime
                    == _subIteration.InternalState.SubIterationState!.StartIngestionTime!.Value;
                var includeEndTime = endTime
                    == _subIteration.InternalState.SubIterationState!.EndIngestionTime!.Value;
                var protoRecordBatches = await LoadProtoRecordBatchAsync(
                    new TimeWindow(startTime, endTime),
                    includeStartTime,
                    includeEndTime);

                if (protoRecordBatches.Any())
                {
                    var extentMap = await FetchExtentIdMapAsync(
                        protoRecordBatches.Select(p => p.ExtentId));

                    if (extentMap.Count() != protoRecordBatches.Count())
                    {
                        Trace.TraceWarning(
                            $"Extent list changed between 2 close operations on "
                            + $"table '{_kustoPriority.TableName}':  "
                            + $"'{protoRecordBatches.Count()}' vs '{extentMap.Count()}'.  "
                            + "Likely cause:  merge.  Mitigation:  retry.");
                    }
                    else
                    {
                        var recordBatches = protoRecordBatches
                            .GroupBy(p => p.ExtentId)
                            .Select(g => StatusItem.CreateRecordBatch(
                                _subIteration.IterationId,
                                _subIteration.SubIterationId!.Value,
                                _recordBatchIdProvider(),
                                _kustoPriority.TableName!,
                                g.Select(p => new TimeInterval
                                {
                                    StartTime = p.IngestionTimeInterval.StartTime,
                                    EndTime = p.IngestionTimeInterval.EndTime
                                }),
                                extentMap[g.Key],
                                g.Sum(p => p.RecordCount)))
                            .ToImmutableArray();

                        await _dbStatus.PersistNewItemsAsync(recordBatches, ct);
                        if (protoRecordBatches.Count() < RECORD_BATCH_SIZE)
                        {
                            return;
                        }
                    }
                }
                else
                {
                    return;
                }
            }
            while (true);
        }

        private async Task<IImmutableDictionary<string, DateTime>> FetchExtentIdMapAsync(
            IEnumerable<string> extentIds)
        {
            var extentIdTextList = string.Join(
                Environment.NewLine + ", ",
                extentIds.Select(e => $"'{e}'"));
            var mapList = await _sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                $@"
.show table ['{_kustoPriority.TableName}'] extents
({extentIdTextList})
| project tostring(ExtentId), MaxCreatedOn
",
                r => new
                {
                    ExtentId = (string)r["ExtentId"],
                    MaxCreatedOn = (DateTime)r["MaxCreatedOn"]
                });
            var map = mapList.ToImmutableDictionary(p => p.ExtentId, p => p.MaxCreatedOn);

            return map;
        }

        private async Task<IImmutableList<ProtoRecordBatch>> LoadProtoRecordBatchAsync(
            TimeWindow timeWindow,
            bool includeStartTime,
            bool includeEndTime)
        {
            var sourceQueuedClient = _sourceQueuedClient
                .SetParameter("StartTime", timeWindow.StartTime)
                .SetParameter("EndTime", timeWindow.EndTime);
            var startTimeOperator = includeStartTime ? ">=" : ">";
            var queryText = $@"
declare query_parameters(StartTime:datetime, EndTime:datetime);
['{_kustoPriority.TableName}']
{_cursorWindowPredicate}
| where ingestion_time() {startTimeOperator} StartTime
//  End time is always excluded
| where ingestion_time() < EndTime
| summarize Cardinality=count() by IngestionTime=ingestion_time(), ExtentId=extent_id()
| order by IngestionTime asc
| extend IsNewExtentId = iif(prev(ExtentId)==ExtentId, false, true)
| extend IsNextNewExtentId = iif(next(ExtentId)==ExtentId, false, true)
| extend TotalCardinality = row_cumsum(Cardinality, IsNewExtentId)
| where IsNewExtentId or IsNextNewExtentId
| extend MinIngestionTime=IngestionTime
| extend MaxIngestionTime=iif(not(IsNextNewExtentId), next(IngestionTime), IngestionTime)
| extend ActualTotalCardinality = iif(IsNextNewExtentId, TotalCardinality, next(TotalCardinality))
| where IsNewExtentId
| project MinIngestionTime, MaxIngestionTime, tostring(ExtentId), Cardinality=ActualTotalCardinality
| take {RECORD_BATCH_SIZE}
";
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
    }
}