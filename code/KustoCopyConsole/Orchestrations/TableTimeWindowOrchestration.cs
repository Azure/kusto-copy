using KustoCopyConsole.KustoQuery;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    public class TableTimeWindowOrchestration
    {
        #region Inner Types
        private record TotalBracket(long Cardinality, DateTime? Min, DateTime? Max);
        #endregion

        private readonly KustoPriority _kustoPriority;
        private readonly string _cursorWindowPredicate;
        private readonly string _startTimePredicate;
        private readonly DateTime? _startTime;
        private readonly long _tableSizeCap;
        private readonly KustoQueuedClient _sourceQueuedClient;

        public record TimeWindow(long Cardinality, DateTime? StartTime, DateTime? EndTime);

        #region Constructors
        public static async Task<IImmutableList<TimeWindow>> ComputeWindowsAsync(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? startTime,
            long tableSizeCap,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TableTimeWindowOrchestration(
                kustoPriority,
                cursorWindow,
                startTime,
                tableSizeCap,
                sourceQueuedClient);
            var windows = await orchestration.ComputeWindowsAsync(ct);

            return windows;
        }

        private TableTimeWindowOrchestration(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? startTime,
            long tableSizeCap,
            KustoQueuedClient sourceQueuedClient)
        {
            _kustoPriority = kustoPriority;
            _cursorWindowPredicate =
                cursorWindow.startCursor == null && cursorWindow.endCursor == null
                ? string.Empty
                : cursorWindow.startCursor == null
                ? $"| where cursor_before_or_at('{cursorWindow.endCursor}')"
                : @$"
| where cursor_before_or_at('{cursorWindow.endCursor}')
| where cursor_after('{cursorWindow.startCursor}')";
            _startTimePredicate = _startTime == null
                ? string.Empty
                : _kustoPriority.IterationId == 1
                ? $"| where ingestion_time() < StartTime"
                : $"| where ingestion_time() > StartTime";
            _startTime = startTime;
            _tableSizeCap = tableSizeCap;
            _sourceQueuedClient = sourceQueuedClient;
        }
        #endregion

        private async Task<IImmutableList<TimeWindow>> ComputeWindowsAsync(CancellationToken ct)
        {
            var totalBracket = await ComputeTotalBracketAsync();

            if (totalBracket.Min == null && totalBracket.Max == null)
            {
                return ImmutableArray<TimeWindow>.Empty.Add(
                    new TimeWindow(totalBracket.Cardinality, null, null));
            }
            else if (totalBracket.Min == null || totalBracket.Max == null)
            {
                throw new NotImplementedException();
            }
            else
            {
                if (totalBracket.Cardinality > _tableSizeCap)
                {
                    throw new NotImplementedException();
                }
                else
                {
                    var windows = await FanOutByDaysAsync();

                    return windows;
                }
            }
        }

        private async Task<IImmutableList<TimeWindow>> FanOutByDaysAsync()
        {
            var startTimeDeclare = _startTime == null
                ? string.Empty
                : "declare query_parameters(StartTime:datetime);";
            var sourceQueuedClient = _startTime == null
                ? _sourceQueuedClient
                : _sourceQueuedClient.SetParameter("StartTime", _startTime.Value);
            var windows = await sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                $@"
{_startTimePredicate}
['{_kustoPriority.TableName}']
{_cursorWindowPredicate}
| summarize Cardinality=count() by StartTime=bin(ingestion_time(), 1d)
| extend EndTime = ingestion_time()+1d
",
                r => new TimeWindow(
                    (long)r["Cardinality"],
                    r["StartTime"].To<DateTime>(),
                    r["EndTime"].To<DateTime>()));

            return windows;
        }

        private async Task<TotalBracket> ComputeTotalBracketAsync()
        {
            var startTimeDeclare = _startTime == null
                ? string.Empty
                : "declare query_parameters(StartTime:datetime);";
            var sourceQueuedClient = _startTime == null
                ? _sourceQueuedClient
                : _sourceQueuedClient.SetParameter("StartTime", _startTime.Value);
            var totalBrackets = await sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                $@"
{_startTimePredicate}
['{_kustoPriority.TableName}']
{_cursorWindowPredicate}
| summarize Cardinality = count(), Min = min(ingestion_time()), Max = max(ingestion_time())
",
                r => new TotalBracket(
                    (long)r["Cardinality"],
                    r["Min"].To<DateTime>(),
                    r["Max"].To<DateTime>()));

            return totalBrackets.First();
        }
    }
}