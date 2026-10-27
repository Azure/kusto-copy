using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using TrackDb.Lib;

namespace KustoCopyConsole.Runner.Source
{
    internal class PlanningRunner : ClusterRunnerBase
    {
        private static readonly TimeSpan POST_PLANNING_TIMEOUT = TimeSpan.FromMinutes(2.5);
        private const int MAX_EXTENT_COUNT = 10000;
        private const int MAX_ACTIVE_BLOCKS = 2000;
        private const long MAX_ROW_COUNT_PER_BLOCK = 16000000;
        private const long MAX_ROW_COUNT_PER_PARTITION = 250 * MAX_ROW_COUNT_PER_BLOCK;
        private const int MAX_LEVEL = 7;

        public PlanningRunner(RunnerParameters parameters)
           : base(parameters, true, TimeSpan.FromSeconds(15))
        {
        }

        public override async Task RunAsync(CancellationToken ct)
        {
            if (ShouldExportRun)
            {
                await base.RunAsync(ct);
            }
        }

        protected override async Task RunClusterAsync(
            Uri clusterUri,
            IEnumerable<string> activityNames,
            bool isRunnerShared,
            CancellationToken ct)
        {
            if (CanPlan())
            {
                foreach (var activityName in activityNames)
                {
                    ct.ThrowIfCancellationRequested();
                    await RunActivityAsync(activityName, ct);
                }
            }
        }

        protected async Task RunActivityAsync(string activityName, CancellationToken ct)
        {
            var activityParam = Parameterization.GetActivity(activityName);
            var iterations = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                .Where(pf => pf.In(i => i.State, [IterationState.Starting, IterationState.Planning]))
                .OrderBy(i => i.IterationKey.IterationId)
                .ToArray();
            var source = activityParam.GetSourceTableIdentity();
            var destination = activityParam.GetDestinationTableIdentity();
            var queryClient = DbClientFactory.GetDbQueryClient(
                source.ClusterUri,
                source.DatabaseName);
            var commandClient = DbClientFactory.GetDbCommandClient(
                source.ClusterUri,
                source.DatabaseName);

            foreach (var iteration in iterations)
            {
                if (!CanPlan())
                {
                    return;
                }
                ct.ThrowIfCancellationRequested();
                await RunIterationAsync(activityParam, iteration, queryClient, commandClient, ct);
            }
        }

        private async Task RunIterationAsync(
            ActivityParameterization activityParam,
            IterationRecord iteration,
            DbQueryClient queryClient,
            DbCommandClient commandClient,
            CancellationToken ct)
        {
            if (iteration.State == IterationState.Starting)
            {
                var cursor = await queryClient.GetCurrentCursorAsync(
                    new KustoPriority(iteration.IterationKey),
                    ct);

                iteration = TransitionToPlanning(iteration, cursor);
            }
            if (iteration.State == IterationState.Planning)
            {
                await MigrateDataAsync(
                    queryClient,
                    activityParam,
                    iteration,
                    ct);
                await PartitionIterationDataAsync(
                    queryClient,
                    commandClient,
                    activityParam,
                    iteration.IterationKey,
                    ct);
            }
        }

        private bool CanPlan()
        {
            var activeBlockCount = Database.BlockMetrics.Query()
                .Where(pf => pf.LessThan(bm => bm.BlockMetric, BlockMetric.ExtentMoved))
                .Sum(bm => bm.Value);

            return activeBlockCount <= MAX_ACTIVE_BLOCKS;
        }

        private async Task MigrateDataAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            CancellationToken ct)
        {
            var partitions = Database.PlanningPartitions.Query()
                .Where(pf => pf.Equal(pp => pp.IterationKey, iteration.IterationKey))
                .ToArray();
            var newPartitions = new List<PlanningPartitionRecord2>(partitions.Length);

            foreach (var partition in partitions)
            {   //  Add extent count to the v2 of PlanningPartitionRecord
                var extentCount = await queryClient.GetExtentCountAsync(
                    new KustoPriority(iteration.IterationKey),
                    activityParam.GetSourceTableIdentity().TableName,
                    activityParam.KqlQuery,
                    iteration.CursorStart,
                    iteration.CursorEnd,
                    partition.MinIngestionTime,
                    partition.MaxIngestionTime,
                    ct);
                var newPartition = new PlanningPartitionRecord2(
                    partition.IterationKey,
                    partition.Level,
                    partition.PartitionId,
                    partition.RowCount,
                    extentCount,
                    partition.MinIngestionTime,
                    partition.MaxIngestionTime);

                newPartitions.Add(newPartition);
            }

            using (var tx = Database.CreateTransaction())
            {
                Database.PlanningPartitions.Query(tx)
                    .Where(pf => pf.Equal(pp => pp.IterationKey, iteration.IterationKey))
                    .Delete();
                Database.PlanningPartitions2.AppendRecords(newPartitions, tx);

                tx.Complete();
            }
        }

        private IterationRecord TransitionToPlanning(IterationRecord iteration, string cursor)
        {
            var newIterationRecord = iteration with
            {
                State = IterationState.Planning,
                CursorEnd = cursor
            };

            Database.Iterations.UpdateRecord(iteration, newIterationRecord);
            iteration = newIterationRecord;

            return iteration;
        }

        private IterationRecord LoadIteration(IterationKey iterationKey)
        {
            var iteration = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                .First();

            return iteration;
        }

        private async Task PartitionIterationDataAsync(
            DbQueryClient queryClient,
            DbCommandClient commandClient,
            ActivityParameterization activityParam,
            IterationKey iterationKey,
            CancellationToken ct)
        {
            PlanningPartitionRecord2? lastPartition = null;

            do
            {
                //  Depth first
                lastPartition = Database.PlanningPartitions2.Query()
                    .Where(pf => pf.Equal(pp => pp.IterationKey, iterationKey))
                    .OrderByDescending(pp => pp.Level)
                    .ThenBy(pp => pp.PartitionId)
                    .Take(1)
                    .FirstOrDefault();

                await SubPartitionAsync(
                    queryClient,
                    commandClient,
                    activityParam,
                    LoadIteration(iterationKey),
                    lastPartition,
                    ct);
            }
            while (CanPlan() && LoadIteration(iterationKey).State == IterationState.Planning);
        }

        private async Task SubPartitionAsync(
            DbQueryClient queryClient,
            DbCommandClient commandClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            PlanningPartitionRecord2? lastPartition,
            CancellationToken ct)
        {
            if (lastPartition == null)
            {   //  Level 1:  breakdown per day
                await InitialBreakdownAsync(
                    queryClient,
                    activityParam,
                    iteration,
                    ct);
            }
            else if (string.IsNullOrWhiteSpace(activityParam.PostPlanningKqlQuery))
            {
                await OpenPartitionWithoutPostPlanningQueryAsync(
                    queryClient,
                    commandClient,
                    activityParam,
                    iteration,
                    lastPartition,
                    ct);
            }
            else
            {
                await OpenPartitionWithPostPlanningQueryAsync(
                    queryClient,
                    activityParam,
                    iteration,
                    lastPartition,
                    ct);
            }
        }

        #region Initial Breakdown
        private async Task InitialBreakdownAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            CancellationToken ct)
        {
            var timeIntervals = await queryClient.InitialPartitionRowsAsync(
                new KustoPriority(iteration.IterationKey),
                activityParam.GetSourceTableIdentity().TableName,
                iteration.CursorStart,
                iteration.CursorEnd,
                GetPartitionResolution(1),
                ct);
            var planningPartitions = timeIntervals
                .Index()
                .Select(ti => new PlanningPartitionRecord2(
                    iteration.IterationKey,
                    1,
                    ti.Index,
                    0,
                    0,
                    ti.Item.MinIngestionTime,
                    ti.Item.MaxIngestionTime))
                .ToArray();

            Database.PlanningPartitions2.AppendRecords(planningPartitions);
            if (planningPartitions.Length == 0)
            {
                PlannedIfPlanningCompleted(iteration.IterationKey);
            }
        }
        #endregion

        #region With Post planning
        private async Task OpenPartitionWithPostPlanningQueryAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            PlanningPartitionRecord2 parentPartition,
            CancellationToken ct)
        {
            if (parentPartition.Level == 1)
            {   //  Let's first go to level 2
                await SubPartitionWithoutPostPlanningQueryAsync(
                    queryClient,
                    activityParam,
                    iteration,
                    parentPartition,
                    ct);
            }
            else
            {   //  Let's try the post planning query 
                var rowCount = await queryClient.GetPostPlanningRowCountAsync(
                    new KustoPriority(iteration.IterationKey),
                    activityParam.GetSourceTableIdentity().TableName,
                    activityParam.KqlQuery,
                    activityParam.PostPlanningKqlQuery,
                    iteration.CursorStart,
                    iteration.CursorEnd,
                    parentPartition.MinIngestionTime,
                    parentPartition.MaxIngestionTime,
                    POST_PLANNING_TIMEOUT,
                    ct);

                if (rowCount == null || rowCount > MAX_ROW_COUNT_PER_BLOCK)
                {   //  Can't evaluate the post planning query:  sub partition further
                    await SubPartitionWithoutPostPlanningQueryAsync(
                        queryClient,
                        activityParam,
                        iteration,
                        parentPartition,
                        ct);
                }
                else
                {
                    var protoBlock = new ProtoBlock(
                        rowCount.Value,
                        parentPartition.MinIngestionTime,
                        parentPartition.MaxIngestionTime,
                        null);

                    PersistBlocks(parentPartition, [protoBlock]);
                }
            }
        }
        #endregion

        #region Without Post planning
        private async Task OpenPartitionWithoutPostPlanningQueryAsync(
            DbQueryClient queryClient,
            DbCommandClient commandClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            PlanningPartitionRecord2 parentPartition,
            CancellationToken ct)
        {
            if (parentPartition.RowCount > 0
                && parentPartition.RowCount <= MAX_ROW_COUNT_PER_PARTITION
                && parentPartition.ExtentCount <= MAX_EXTENT_COUNT)
            {   //  Ready to actually load blocks
                await LoadBlocksWithoutPostPlanningQueryAsync(
                    queryClient,
                    commandClient,
                    activityParam,
                    iteration,
                    parentPartition,
                    ct);
            }
            else
            {   //  Sub partition further
                await SubPartitionWithoutPostPlanningQueryAsync(
                    queryClient,
                    activityParam,
                    iteration,
                    parentPartition,
                    ct);
            }
        }

        private async Task SubPartitionWithoutPostPlanningQueryAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            PlanningPartitionRecord2 parentPartition,
            CancellationToken ct)
        {
            if (parentPartition.Level >= MAX_LEVEL)
            {
                throw new InvalidOperationException(
                    "We reached the deepest allowed planning level");
            }
            var timeIntervals = await queryClient.SubPartitionRowsAsync(
                new KustoPriority(iteration.IterationKey),
                activityParam.GetSourceTableIdentity().TableName,
                activityParam.KqlQuery,
                iteration.CursorStart,
                iteration.CursorEnd,
                parentPartition.MinIngestionTime,
                parentPartition.MaxIngestionTime,
                GetPartitionResolution(parentPartition.Level + 1),
                MAX_ROW_COUNT_PER_PARTITION,
                ct);
            var planningPartitions = timeIntervals
                .Index()
                .Select(ti => new PlanningPartitionRecord2(
                    iteration.IterationKey,
                    parentPartition.Level + 1,
                    ti.Index,
                    0,
                    0,
                    ti.Item.MinIngestionTime,
                    ti.Item.MaxIngestionTime))
                .ToArray();

            using (var tx = Database.CreateTransaction())
            {
                DeletePartition(parentPartition, tx);
                Database.PlanningPartitions2.AppendRecords(planningPartitions, tx);
                PlannedIfPlanningCompleted(iteration.IterationKey, tx);

                tx.Complete();
            }
        }

        private async Task LoadBlocksWithoutPostPlanningQueryAsync(
            DbQueryClient queryClient,
            DbCommandClient commandClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            PlanningPartitionRecord2 parentPartition,
            CancellationToken ct)
        {
            var protoBlocks = await LoadProtoBlocksWithoutPostPlanningQueryAsync(
                queryClient,
                commandClient,
                activityParam,
                iteration,
                parentPartition,
                ct);

            PersistBlocks(parentPartition, protoBlocks);
        }

        private async Task<IEnumerable<ProtoBlock>> LoadProtoBlocksWithoutPostPlanningQueryAsync(
            DbQueryClient queryClient,
            DbCommandClient commandClient,
            ActivityParameterization activityParam,
            IterationRecord iteration,
            PlanningPartitionRecord2 parentPartition,
            CancellationToken ct)
        {
            while (true)
            {
                var extentIds = await queryClient.GetExtentIdsAsync(
                    new KustoPriority(parentPartition.IterationKey),
                    activityParam.GetSourceTableIdentity().TableName,
                    activityParam.KqlQuery,
                    iteration.CursorStart,
                    iteration.CursorEnd,
                    parentPartition.MinIngestionTime,
                    parentPartition.MaxIngestionTime,
                    ct);
                var extentCreationTimes = await commandClient.GetExtentCreationTimeAsync(
                    new KustoPriority(parentPartition.IterationKey),
                    activityParam.GetSourceTableIdentity().TableName,
                    extentIds,
                    ct);

                if (extentIds.Count() == extentCreationTimes.Count())
                {
                    var protoBlocks = await queryClient.GetProtoBlocksAsync(
                        new KustoPriority(parentPartition.IterationKey),
                        activityParam.GetSourceTableIdentity().TableName,
                        activityParam.KqlQuery,
                        iteration.CursorStart,
                        iteration.CursorEnd,
                        parentPartition.MinIngestionTime,
                        parentPartition.MaxIngestionTime,
                        extentCreationTimes,
                        TimeSpan.FromSeconds(0.01),
                        MAX_ROW_COUNT_PER_BLOCK,
                        ct);

                    return protoBlocks;
                }
                else
                {
                    Trace.TraceWarning(
                        $"Extent count mismatch:  {extentIds.Count()} extents," +
                        $" {extentCreationTimes.Count()} creation times");
                }
            }
        }
        #endregion

        private void PersistBlocks(
            PlanningPartitionRecord2 parentPartition,
            IEnumerable<ProtoBlock> protoBlocks)
        {
            using (var tx = Database.CreateTransaction())
            {
                DeletePartition(parentPartition, tx);
                if (protoBlocks.Count() > 0)
                {
                    var iteration = Database.Iterations.Query(tx)
                        .Where(pf => pf.Equal(i => i.IterationKey, parentPartition.IterationKey))
                        .First();
                    var nextBlockId = iteration.NextBlockId;
                    var blocks = protoBlocks
                        .Select(p => new BlockRecord(
                            BlockState.Planned,
                            new BlockKey(iteration.IterationKey, nextBlockId++),
                            p.MinIngestionTime,
                            p.MaxIngestionTime,
                            p.CreationTime,
                            p.RowCount,
                            0,
                            string.Empty,
                            string.Empty,
                            string.Empty))
                        .ToArray();

                    Database.Blocks.AppendRecords(blocks, tx);
                    Database.Iterations.UpdateRecord(
                        iteration,
                        iteration with { NextBlockId = nextBlockId },
                        tx);
                }
                PlannedIfPlanningCompleted(parentPartition.IterationKey, tx);

                tx.Complete();
            }
        }

        private void DeletePartition(PlanningPartitionRecord2? partition, TransactionContext tx)
        {
            if (partition != null)
            {
                Database.PlanningPartitions2.Query(tx)
                    .Where(pf => pf.Equal(pp => pp.IterationKey, partition.IterationKey))
                    .Where(pf => pf.Equal(pp => pp.Level, partition.Level))
                    .Where(pf => pf.Equal(pp => pp.PartitionId, partition.PartitionId))
                    .Delete();
            }
        }

        private TimeSpan GetPartitionResolution(int level)
        {
            return level switch
            {
                1 => TimeSpan.FromDays(1),
                2 => TimeSpan.FromHours(1),
                3 => TimeSpan.FromMinutes(1),
                4 => TimeSpan.FromSeconds(1),
                5 => TimeSpan.FromMilliseconds(100),
                6 => TimeSpan.FromMilliseconds(10),
                7 => TimeSpan.FromMilliseconds(1),
                _ => throw new NotSupportedException($"Level {level}")
            };
        }

        private void PlannedIfPlanningCompleted(
            IterationKey iterationKey,
            TransactionContext? tx = null)
        {
            var planning = Database.PlanningPartitions2.Query(tx)
                .Where(pf => pf.Equal(pp => pp.IterationKey, iterationKey))
                .Take(1)
                .FirstOrDefault();

            if (planning == null)
            {   //  We cleared all partitions:  we're done
                var iteration = Database.Iterations.Query(tx)
                    .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                    .First();

                if (iteration.State != IterationState.Planning)
                {
                    Database.Iterations.UpdateRecord(
                        iteration,
                        iteration with { State = IterationState.Planned },
                        tx);
                }
            }
        }
    }
}