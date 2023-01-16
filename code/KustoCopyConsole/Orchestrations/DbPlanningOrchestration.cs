using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Runtime.InteropServices;

namespace KustoCopyConsole.Orchestrations
{
    public partial class DbPlanningOrchestration
    {
        #region Inner Types
        private record DbTimespanConfig(
            TimeSpan Rpo,
            TimeSpan? BackfillHorizon);

        private record TableTimeWindowCounts(
            string TableName,
            IImmutableList<TimeWindowCount> TimeWindowCounts);

        private record TableProtoPlanning(
            string TableName,
            DateTime? IterationTableEndTime,
            IImmutableList<PlanRecordBatchState> RecordBatches);

        private record TablePlanning(
            string TableName,
            IImmutableList<PlanRecordBatchState> RecordBatches);
        #endregion

        private const long MIN_EXTENT_COUNT = 5;
        private const long MAX_EXTENT_COUNT = 10;

        private readonly bool _isContinuousRun;
        private readonly SourceDatabaseParameterization _dbParameterization;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _queuedClient;

        #region Constructor
        public static async Task PlanAsync(
            bool isContinuousRun,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoQueuedClient? queuedClient,
            CancellationToken ct)
        {
            if (queuedClient == null)
            {
                return;
            }
            else
            {
                var orchestration = new DbPlanningOrchestration(
                    isContinuousRun,
                    dbParameterization,
                    dbStatus,
                    queuedClient);

                await orchestration.RunAsync(ct);
            }
        }

        private DbPlanningOrchestration(
            bool isContinuousRun,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoQueuedClient queuedClient)
        {
            _isContinuousRun = isContinuousRun;
            _dbParameterization = dbParameterization;
            _dbStatus = dbStatus;
            _queuedClient = queuedClient;
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            var dbTimespanConfig = await FetchDbTimespanConfigAsync();

            do
            {
                var iteration = await ComputeIncompleteIterationAsync(dbTimespanConfig.Rpo, ct);
                var tableNames = await ComputeTableNamesAsync();

                do
                {
                    await PlanNewSubIterationAsync(
                        iteration,
                        tableNames,
                        dbTimespanConfig.BackfillHorizon,
                        ct);
                }
                while (_dbStatus.GetIteration(iteration.IterationId).State
                == StatusItemState.Initial);
            }
            while (_isContinuousRun);
        }

        private async Task<DbTimespanConfig> FetchDbTimespanConfigAsync()
        {
            var queryText = @$"print
BackfillHorizon=timespan({_dbParameterization.DatabaseOverrides!.BackfillHorizon ?? "null"}),
Rpo=timespan({_dbParameterization.DatabaseOverrides!.Rpo})";
            var outputs = await _queuedClient.ExecuteQueryAsync(
                KustoPriority.HighestPriority,
                _dbParameterization.Name!,
                queryText,
                r => new
                {
                    BackfillHorizon = r["BackfillHorizon"].To<TimeSpan>(),
                    Rpo = (TimeSpan)r["Rpo"]
                });
            var output = outputs.First();

            return new DbTimespanConfig(output.Rpo, output.BackfillHorizon);
        }

        private async Task PlanNewSubIterationAsync(
            StatusItem iteration,
            IImmutableList<string> tableNames,
            TimeSpan? backfillHorizon,
            CancellationToken ct)
        {
            var newSubIterationId = _dbStatus.GetNewSubIterationId(iteration.IterationId);

            if (iteration.IterationId == 1 && newSubIterationId == 1 && backfillHorizon != null)
            {
                var newSubIteration = StatusItem.CreateSubIteration(
                    iteration.IterationId,
                    newSubIterationId,
                    DateTime.Now.ToUtc().Subtract(backfillHorizon!.Value));

                await _dbStatus.PersistNewItemsAsync(new[] { newSubIteration }, ct);
            }
            else
            {
                var tablePlannings =
                    await PlanTablesAsync(iteration, newSubIterationId, tableNames, ct);

                if (tablePlannings.Any())
                {
                    var maxIterationEndTime = tablePlannings
                        .Where(t => t.IterationTableEndTime != null)
                        .Select(t => t.IterationTableEndTime!.Value)
                        .Max();
                    var maxSubIterationEndTime = ComputeMaxSubIterationEndTime(
                        tablePlannings,
                        maxIterationEndTime);
                    var clippedPlannings = ClipPlannings(tablePlannings, maxSubIterationEndTime);
                    var newSubIteration = StatusItem.CreateSubIteration(
                        iteration.IterationId,
                        newSubIterationId,
                        maxSubIterationEndTime);
                    long currentRecordId = 1;
                    var recordBatches = clippedPlannings
                        //  Align record batch IDs with table names
                        .OrderBy(p => p.TableName)
                        .Select(p => p.RecordBatches
                        .Select(r => StatusItem.CreateRecordBatch(
                            iteration.IterationId,
                            newSubIterationId,
                            currentRecordId++,
                            p.TableName,
                            r.IngestionTimes,
                            r.CreationTime!.Value,
                            r.RecordCount)))
                        .SelectMany(r => r)
                        .ToImmutableArray();
                    //  We define it but might not use it
                    var plannedIteration = iteration.UpdateState(StatusItemState.Planned);
                    var items = maxIterationEndTime == maxSubIterationEndTime
                        ? recordBatches.Prepend(newSubIteration).Append(plannedIteration)
                        : recordBatches.Prepend(newSubIteration);

                    Trace.WriteLine($"Sub Iteration {newSubIterationId} planned");
                    await _dbStatus.PersistNewItemsAsync(items, ct);
                }
                else
                {
                    var plannedIteration = iteration.UpdateState(StatusItemState.Planned);

                    await _dbStatus.PersistNewItemsAsync(new[] { plannedIteration }, ct);
                }
            }
        }

        private async Task<IImmutableList<TableProtoPlanning>> PlanTablesAsync(
            StatusItem iteration,
            long newSubIterationId,
            IImmutableList<string> tableNames,
            CancellationToken ct)
        {
            var cursorWindow = _dbStatus.GetCursorWindow(iteration.IterationId);
            var previousSubIteration =
                _dbStatus.GetSubIterations(iteration.IterationId).LastOrDefault();
            var startIngestionTimeExclusive = previousSubIteration?.SubIterationEndTime;
            var tablePlanningTasks = tableNames
                .Select(t => PlanTableAsync(
                    iteration,
                    t,
                    newSubIterationId,
                    cursorWindow,
                    startIngestionTimeExclusive,
                    ct))
                .ToImmutableArray();

            await Task.WhenAll(tablePlanningTasks);

            var tablePlannings = tablePlanningTasks
                .Select(t => t.Result)
                .Where(p => p.RecordBatches.Any())
                .ToImmutableArray();

            return tablePlannings;
        }

        private static DateTime ComputeMaxSubIterationEndTime(
            IImmutableList<TableProtoPlanning> tablePlannings,
            DateTime maxIterationEndTime)
        {
            //  If no more records, that table doesn't constrain the sub iteration
            var incompletePlannings = ComputeIncompletePlannings(tablePlannings);

            if (incompletePlannings.Any())
            {
                //  Find a clipping value
                var maxSubIterationEndTime = incompletePlannings
                    .Select(p => p.RecordBatches)
                    .Select(r => r.SelectMany(r => r.IngestionTimes))
                    .Select(i => i.Select(ii => ii.EndTime).Max())
                    .Min();

                return maxSubIterationEndTime;
            }
            else
            {
                return maxIterationEndTime;
            }
        }

        private static IEnumerable<TableProtoPlanning> ComputeIncompletePlannings(
            IEnumerable<TableProtoPlanning> tablePlannings)
        {
            var incompletePlannings = tablePlannings
                .Select(p => new
                {
                    TablePlanning = p,
                    PlanningEndTime = p
                    .RecordBatches
                    .SelectMany(r => r.IngestionTimes.Select(i => i.EndTime))
                    .Max()
                })
                .Select(b => new
                {
                    TablePlanning = b.TablePlanning,
                    HasMoreRecords = b.PlanningEndTime < b.TablePlanning.IterationTableEndTime
                })
                .Where(p => p.HasMoreRecords)
                .Select(p => p.TablePlanning);

            return incompletePlannings;
        }

        private async Task<TableProtoPlanning> PlanTableAsync(
            StatusItem iteration,
            string t,
            long newSubIterationId,
            CursorWindow cursorWindow,
            DateTime? startIngestionTimeExclusive,
            CancellationToken ct)
        {
            var output = await TablePlanningOrchestration.PlanAsync(
                new KustoPriority(
                    iteration.IterationId,
                    newSubIterationId,
                    _dbStatus.DbName,
                    t),
                cursorWindow,
                startIngestionTimeExclusive,
                MIN_EXTENT_COUNT,
                MAX_EXTENT_COUNT,
                _queuedClient,
                ct);
            var planning = new TableProtoPlanning(
                t,
                output.IterationTableEndTime,
                output.RecordBatches);

            return planning;
        }

        private IImmutableList<TablePlanning> ClipPlannings(
            IEnumerable<TableProtoPlanning> plannings,
            DateTime maxSubIterationEndTime)
        {
            var clippedPlannings = plannings
                .Select(p => new TablePlanning(
                    p.TableName,
                    p.RecordBatches
                    .Select(r => ClipBatch(r, maxSubIterationEndTime))
                    //  Remove batches with no ingestion times
                    .Where(r => r.IngestionTimes.Any())
                    .ToImmutableArray()))
                //  Remove tables with no batches
                .Where(p => p.RecordBatches.Any())
                .ToImmutableArray();

            return clippedPlannings;
        }

        private PlanRecordBatchState ClipBatch(
            PlanRecordBatchState recordBatch,
            DateTime maxSubIterationEndTime)
        {
            var maxBatchEndTime = recordBatch.IngestionTimes
                .Max(i => i.EndTime);

            if (maxBatchEndTime > maxSubIterationEndTime)
            {
                var clippedRecordBatch = new PlanRecordBatchState
                {
                    CreationTime = recordBatch.CreationTime,
                    //  Since we clip, we won't be able to validate record count
                    RecordCount = null,
                    IngestionTimes = recordBatch.IngestionTimes
                    //  Remove ingestion time intervals "above the line"
                    .Where(i => i.StartTime <= maxSubIterationEndTime)
                    .Select(i => new TimeInterval
                    {   //  Clipping both start and end time
                        StartTime = i.StartTime < maxSubIterationEndTime
                        ? i.StartTime
                        : maxSubIterationEndTime,
                        EndTime = i.EndTime < maxSubIterationEndTime
                        ? i.EndTime
                        : maxSubIterationEndTime
                    })
                    .ToImmutableArray()
                };

                return clippedRecordBatch;
            }
            else
            {   //  Nothing to clip
                return recordBatch;
            }
        }

        private async Task<StatusItem> ComputeIncompleteIterationAsync(
            TimeSpan rpo,
            CancellationToken ct)
        {
            var firstIncompleteIteration = _dbStatus.GetIterations()
                .FirstOrDefault(i => i.State == StatusItemState.Initial);

            if (firstIncompleteIteration != null)
            {
                return firstIncompleteIteration;
            }
            else
            {
                var lastIteration = _dbStatus.GetIterations().LastOrDefault();
                var elapsed = DateTime.Now.ToUtc()
                    .Subtract(lastIteration?.Created ?? DateTime.MinValue);
                //  Estimate the time to start planning at half the RPO
                var waitPeriod = (rpo / 2).Subtract(elapsed);

                if (waitPeriod > TimeSpan.Zero)
                {   //  Wait for RPO
                    await Task.Delay(waitPeriod, ct);
                }

                var newIterationId = _dbStatus.GetNewIterationId();
                var cursors = await _queuedClient.ExecuteQueryAsync(
                    KustoPriority.HighestPriority,
                    _dbStatus.DbName,
                    "print Cursor=cursor_current()",
                    r => (string)r["Cursor"]);
                var newIteration = StatusItem.CreateIteration(newIterationId, cursors.First());

                Trace.WriteLine($"Iteration {newIterationId} created");
                await _dbStatus.PersistNewItemsAsync(new[] { newIteration }, ct);

                return newIteration;
            }
        }

        private async Task<IImmutableList<string>> ComputeTableNamesAsync()
        {
            var existingTableNames = await _queuedClient.ExecuteQueryAsync(
                new KustoPriority(),
                _dbStatus.DbName,
                ".show tables | project TableName",
                r => (string)r[0]);

            if (!_dbParameterization.TablesToInclude.Any()
                && !_dbParameterization.TablesToExclude.Any())
            {
                return existingTableNames;
            }
            else if (_dbParameterization.TablesToInclude.Any()
                && !_dbParameterization.TablesToExclude.Any())
            {
                var intersectionTableNames = _dbParameterization.TablesToInclude
                    .Intersect(existingTableNames)
                    .ToImmutableArray();

                return intersectionTableNames;
            }
            else
            {

                var exclusionTableNames = existingTableNames
                    .Except(_dbParameterization.TablesToExclude)
                    .ToImmutableArray();

                return exclusionTableNames;
            }
        }
    }
}