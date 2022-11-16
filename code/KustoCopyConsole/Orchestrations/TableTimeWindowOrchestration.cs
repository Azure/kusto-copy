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
        private readonly string _subIterationStartTimePredicate;
        private readonly string _subIterationEndTimePredicate;
        private readonly DateTime? _subIterationStartTime;
        private readonly DateTime? _subIterationEndTime;
        private readonly long _tableSizeCap;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructors
        public static async Task<IImmutableList<TimeWindowCount>> ComputeWindowsAsync(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? subIterationStartTime,
            DateTime? subIterationEndTime,
            long tableSizeCap,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TableTimeWindowOrchestration(
                kustoPriority,
                cursorWindow,
                subIterationStartTime,
                subIterationEndTime,
                tableSizeCap,
                sourceQueuedClient);
            var windows = await orchestration.ComputeWindowsAsync(ct);

            return windows;
        }

        private TableTimeWindowOrchestration(
            KustoPriority kustoPriority,
            CursorWindow cursorWindow,
            DateTime? subIterationStartTime,
            DateTime? subIterationEndTime,
            long tableSizeCap,
            KustoQueuedClient sourceQueuedClient)
        {
            _kustoPriority = kustoPriority;
            _cursorWindowPredicate = cursorWindow.ToCursorKustoPredicate();
            _subIterationStartTimePredicate = subIterationStartTime == null
                ? string.Empty
                : "| where StartTime > SubIterationStartTime";
            _subIterationEndTimePredicate = subIterationEndTime == null
                ? string.Empty
                : "| where StartTime < SubIterationEndTime";
            _subIterationStartTime = subIterationStartTime;
            _subIterationEndTime = subIterationEndTime;
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
            else if (totalBracket.Cardinality > 0)
            {
                var windows = await FanOutByDaysAsync();

                return windows;
            }
            else
            {
                return ImmutableArray<TimeWindowCount>.Empty;
            }
        }

        private async Task<IImmutableList<TimeWindowCount>> FanOutByDaysAsync()
        {
            var startTimeDeclare = _subIterationStartTime == null
                ? string.Empty
                : "declare query_parameters(StartTime:datetime);";
            var sourceQueuedClient = _sourceQueuedClient;

            sourceQueuedClient = _subIterationStartTime == null
                ? _sourceQueuedClient
                : _sourceQueuedClient.SetParameter(
                    "SubIterationStartTime",
                    _subIterationStartTime.Value);
            sourceQueuedClient = _subIterationEndTime == null
                ? _sourceQueuedClient
                : _sourceQueuedClient.SetParameter(
                    "SubIterationEndTime",
                    _subIterationEndTime.Value);
            var queryText = $@"
declare query_parameters(
    SubIterationStartTime:datetime=datetime(null),
    SubIterationEndTime:datetime=datetime(null));
['{_kustoPriority.TableName}']
{_cursorWindowPredicate}
| project StartTime=bin(ingestion_time(), 1d)
{_subIterationStartTimePredicate}
{_subIterationEndTimePredicate}
| where isnotnull(StartTime)
| summarize Cardinality=count() by StartTime
| extend EndTime = StartTime+1d
";
            var windows = await sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                queryText,
                r => new TimeWindowCount(
                    new TimeWindow((DateTime)r["StartTime"], (DateTime)r["EndTime"]),
                    (long)r["Cardinality"]));

            return windows;
        }

        private async Task<TimeWindowCount> ComputeBracketAsync()
        {
            var startTimeDeclare = _subIterationStartTime == null
                ? string.Empty
                : "declare query_parameters(StartTime:datetime);";
            var sourceQueuedClient = _subIterationStartTime == null
                ? _sourceQueuedClient
                : _sourceQueuedClient.SetParameter("StartTime", _subIterationStartTime.Value);
            var bracketList = await sourceQueuedClient.ExecuteQueryAsync(
                _kustoPriority,
                _kustoPriority.DatabaseName!,
                $@"
{_subIterationStartTimePredicate}
['{_kustoPriority.TableName}']
{_cursorWindowPredicate}
| where isnotnull(ingestion_time())
| summarize Cardinality = count(), Min = min(ingestion_time()), Max = max(ingestion_time())
",
                r => new
                {
                    Cardinality = (long)r["Cardinality"],
                    Min = r["Min"].To<DateTime>(),
                    Max = r["Max"].To<DateTime>()
                });
            var bracket = bracketList.First();

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