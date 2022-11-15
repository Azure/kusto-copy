using KustoCopyConsole.KustoQuery;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    public class TableTimeWindowOrchestration
    {
        #region Inner Types
        #endregion

        private readonly KustoPriority _kustoPriority;
        private readonly string _cursorWindowPredicate;
        private readonly string _startTimePredicate;
        private readonly DateTime? _startTime;
        private readonly long _tableSizeCap;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructors
        public static async Task<IImmutableList<TimeWindowCount>> ComputeWindowsAsync(
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

        private async Task<IImmutableList<TimeWindowCount>> ComputeWindowsAsync(
            CancellationToken ct)
        {
            var totalBracket = await ComputeBracketAsync();

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

        private async Task<IImmutableList<TimeWindowCount>> FanOutByDaysAsync()
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
| where isnotnull(ingestion_time())
| summarize Cardinality=count() by StartTime=bin(ingestion_time(), 1d)
| extend EndTime = StartTime+1d
",
                r => new TimeWindowCount(
                    new TimeWindow((DateTime)r["StartTime"], (DateTime)r["EndTime"]),
                    (long)r["Cardinality"]));

            return windows;
        }

        private async Task<TimeWindowCount> ComputeBracketAsync()
        {
            var startTimeDeclare = _startTime == null
                ? string.Empty
                : "declare query_parameters(StartTime:datetime);";
            var sourceQueuedClient = _startTime == null
                ? _sourceQueuedClient
                : _sourceQueuedClient.SetParameter("StartTime", _startTime.Value);
            var bracketList = await sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                $@"
{_startTimePredicate}
['{_kustoPriority.TableName}']
{_cursorWindowPredicate}
| where isnotnull(ingestion_time())
| summarize Cardinality = count(), Min = min(ingestion_time()), Max = max(ingestion_time())
",
                r => new
                {
                    Cardinality = (long)r["Cardinality"],
                    Min =r["Min"].To<DateTime>(),
                    Max = r["Max"].To<DateTime>()
                });
            var bracket = bracketList.First();
            //new TimeWindowCount(
            //        new TimeWindow((DateTime)r["Min"], (DateTime)r["Max"]),
            //        )

            return bracket.Cardinality == 0
                ? new TimeWindowCount(
                    new TimeWindow(DateTime.MinValue, DateTime.MaxValue),
                    bracket.Cardinality)
                : new TimeWindowCount(
                    new TimeWindow(bracket.Min!.Value, bracket.Max!.Value),
                    bracket.Cardinality);
        }
    }
}