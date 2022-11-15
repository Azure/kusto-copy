using KustoCopyConsole.KustoQuery;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    public class TableTimeWindowOrchestration
    {
        private readonly string _tableName;
        private readonly CursorWindow _cursorWindow;
        private readonly DateTime? _startTime;
        private readonly bool _goBackward;
        private readonly long _tableSizeCap;
        private readonly KustoQueuedClient _sourceQueuedClient;

        public record TimeWindow(DateTime startTime, DateTime endTime, long cardinality);

        #region Constructors
        public static async Task<IImmutableList<TimeWindow>> ComputeWindowsAsync(
            string tableName,
            CursorWindow cursorWindow,
            DateTime? startTime,
            bool goBackward,
            long tableSizeCap,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TableTimeWindowOrchestration(
                tableName,
                cursorWindow,
                startTime,
                goBackward,
                tableSizeCap,
                sourceQueuedClient);
            var windows = await orchestration.ComputeWindowsAsync(ct);

            return windows;
        }

        private TableTimeWindowOrchestration(
            string tableName,
            CursorWindow cursorWindow,
            DateTime? startTime,
            bool goBackward,
            long tableSizeCap,
            KustoQueuedClient sourceQueuedClient)
        {
            _tableName = tableName;
            _cursorWindow = cursorWindow;
            _startTime = startTime;
            _goBackward = goBackward;
            _tableSizeCap = tableSizeCap;
            _sourceQueuedClient = sourceQueuedClient;
        }
        #endregion

        private Task<IImmutableList<TimeWindow>> ComputeWindowsAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}