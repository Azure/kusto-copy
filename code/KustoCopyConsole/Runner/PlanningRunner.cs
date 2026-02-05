using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib;

namespace KustoCopyConsole.Runner
{
    internal class PlanningRunner : ActivityRunnerBase
    {
        #region Inner Types
        private record ExtentBatch(
            DateTime MinIngestionTime,
            DateTime MaxIngestionTime,
            long RecordCount);
        #endregion

        private const int MAX_ACTIVE_BLOCKS_PER_ITERATION = 400;
        private const int MIN_ACTIVE_BLOCKS_PER_ITERATION = 200;
        private const int MAX_ROW_COUNT_PER_BLOCK = 4000000;
        private const int MAX_ROW_COUNT_BY_PARTITION = 64 * MAX_ROW_COUNT_PER_BLOCK;

        public PlanningRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(15))
        {
        }

        protected override async Task<bool> RunActivityAsync(string activityName, CancellationToken ct)
        {
            var iterations = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                .Where(pf => pf.In(i => i.State, [IterationState.Starting, IterationState.Planning]))
                .ToImmutableArray();

            foreach (var iteration in iterations)
            {
                await PlanIterationAsync(iteration, ct);
            }

            return iterations.Any();
        }

        private async Task PlanIterationAsync(IterationRecord iteration, CancellationToken ct)
        {
            var activity = Parameterization.Activities[iteration.IterationKey.ActivityName];
            var source = activity.GetSourceTableIdentity();
            var destination = activity.GetDestinationTableIdentity();
            var queryClient = DbClientFactory.GetDbQueryClient(
                source.ClusterUri,
                source.DatabaseName);

            if (iteration.State == IterationState.Starting)
            {
                var cursor = await queryClient.GetCurrentCursorAsync(
                    new KustoPriority(iteration.IterationKey),
                    ct);

                iteration = PlanningIteration(iteration, cursor);
            }
            if (iteration.State == IterationState.Planning && ShouldPlan(iteration.IterationKey))
            {
                await PlanBlocksAsync(queryClient, activity, iteration.IterationKey, ct);
            }
        }

        #region Planning conditions
        private long GetActiveBlockCount(IterationKey iterationKey)
        {
            return Database.QueryAggregatedBlockMetrics(iterationKey)
                .Where(p => p.Key < BlockMetric.ExtentMoved)
                .Sum(p => p.Value);
        }

        private bool ShouldPlan(IterationKey iterationKey)
        {
            var activeBlockCount = GetActiveBlockCount(iterationKey);

            return activeBlockCount < MIN_ACTIVE_BLOCKS_PER_ITERATION;
        }

        private bool CanKeepPlanning(IterationKey iterationKey)
        {
            var activeBlockCount = GetActiveBlockCount(iterationKey);

            return activeBlockCount < MAX_ACTIVE_BLOCKS_PER_ITERATION;
        }
        #endregion

        private IterationRecord PlanningIteration(IterationRecord iteration, string cursor)
        {
            using (var tx = Database.CreateTransaction())
            {
                var newIterationRecord = iteration with
                {
                    State = IterationState.Planning,
                    CursorEnd = cursor
                };

                Database.Iterations.UpdateRecord(iteration, newIterationRecord, tx);
                Database.TempTables.AppendRecord(
                    new TempTableRecord(
                        TempTableState.Required,
                        iteration.IterationKey,
                        string.Empty),
                    tx);
                iteration = newIterationRecord;

                tx.Complete();
            }

            return iteration;
        }

        private async Task PlanBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationKey iterationKey,
            CancellationToken ct)
        {
            PlanningPartitionRecord? lastPlanningPartition = null;

            do
            {
                lastPlanningPartition = Database.PlanningPartitions.Query()
                    .Where(pf => pf.Equal(pp => pp.IterationKey, iterationKey))
                    .OrderByDescending(pp => pp.Generation)
                    .Take(1)
                    .FirstOrDefault();
            }
            while (await PlanPartitionAsync(
                queryClient,
                activity,
                iterationKey,
                lastPlanningPartition,
                ct)
            && CanKeepPlanning(iterationKey));
        }

        private async Task<bool> PlanPartitionAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationKey iterationKey,
            PlanningPartitionRecord? lastPlanningPartition,
            CancellationToken ct)
        {
            if (lastPlanningPartition == null)
            {
                return await InitialPartitionAsync(
                    queryClient,
                    activity,
                    iterationKey,
                    ct);
            }
            else
            {
                return await SubPlanPartitionAsync(
                    queryClient,
                    activity,
                    iterationKey,
                    lastPlanningPartition,
                    ct);
            }
        }

        private async Task<bool> InitialPartitionAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationKey iterationKey,
            CancellationToken ct)
        {
            var iteration = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                .First();
            var stats = await queryClient.GetRecordStatsAsync(
                new KustoPriority(iterationKey),
                activity.GetSourceTableIdentity().TableName,
                activity.KqlQuery,
                iteration.CursorStart,
                iteration.CursorEnd,
                null,
                null,
                ct);

            if (stats.RecordCount > 0)
            {
                var planningPartition = new PlanningPartitionRecord(
                    iteration.IterationKey,
                    0,
                    false,
                    stats.RecordCount,
                    stats.MinIngestionTime,
                    stats.MedianIngestionTime,
                    stats.MaxIngestionTime);

                Database.PlanningPartitions.AppendRecord(planningPartition);

                return true;
            }
            else
            {
                return false;
            }
        }

        private async Task<bool> SubPlanPartitionAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationKey iterationKey,
            PlanningPartitionRecord lastPlanningPartition,
            CancellationToken ct)
        {
            if (lastPlanningPartition.RecordCount > MAX_ROW_COUNT_BY_PARTITION)
            {   //  Sub partition
                var iteration = Database.Iterations.Query()
                    .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                    .First();
                var stats = await queryClient.GetRecordStatsAsync(
                    new KustoPriority(iterationKey),
                    activity.GetSourceTableIdentity().TableName,
                    activity.KqlQuery,
                    iteration.CursorStart,
                    iteration.CursorEnd,
                    !lastPlanningPartition.IsLeftExplored
                    ? new DateTimeBoundary(lastPlanningPartition.MinIngestionTime, true)
                    //  Exclude median
                    : new DateTimeBoundary(lastPlanningPartition.MedianIngestionTime, false),
                    !lastPlanningPartition.IsLeftExplored
                    //  Include median
                    ? new DateTimeBoundary(lastPlanningPartition.MedianIngestionTime, true)
                    : new DateTimeBoundary(lastPlanningPartition.MaxIngestionTime, true),
                    ct);

                if (stats.RecordCount > 0)
                {
                    var planningPartition = new PlanningPartitionRecord(
                        iteration.IterationKey,
                        lastPlanningPartition.Generation + 1,
                        false,
                        stats.RecordCount,
                        stats.MinIngestionTime,
                        stats.MedianIngestionTime,
                        stats.MaxIngestionTime);

                    Database.PlanningPartitions.AppendRecord(planningPartition);

                    return true;
                }
                else
                {
                    using (var tx = Database.CreateTransaction())
                    {
                        var result = ClearOneSide(lastPlanningPartition, false, tx);

                        tx.Complete();

                        return result;
                    }
                }
            }
            else
            {   //  Harvest blocks
                return await LoadBlocksAsync(
                    queryClient,
                    activity,
                    iterationKey,
                    lastPlanningPartition,
                    ct);
            }
        }

        private bool ClearOneSide(
            PlanningPartitionRecord planningPartition,
            bool doNotSplit,
            TransactionContext tx)
        {
            if (!doNotSplit && !planningPartition.IsLeftExplored)
            {   //  We clear the left side
                Database.PlanningPartitions.UpdateRecord(
                    planningPartition,
                    planningPartition with { IsLeftExplored = true },
                    tx);

                return true;
            }
            else
            {   //  We clear the right side, so we go up
                Database.PlanningPartitions.Query(tx)
                    .Where(pf => pf.Equal(pp => pp.IterationKey, planningPartition.IterationKey))
                    .Where(pf => pf.Equal(pp => pp.Generation, planningPartition.Generation))
                    .Delete();

                var lastPlanningPartition = Database.PlanningPartitions.Query(tx)
                    .Where(pf => pf.Equal(pp => pp.IterationKey, planningPartition.IterationKey))
                    .OrderByDescending(pp => pp.Generation)
                    .Take(1)
                    .FirstOrDefault();

                if (lastPlanningPartition == null)
                {   //  We cleared all partitions:  we're done
                    var iteration = Database.Iterations.Query(tx)
                        .Where(pf => pf.Equal(i => i.IterationKey, planningPartition.IterationKey))
                        .First();

                    Database.Iterations.UpdateRecord(
                        iteration,
                        iteration with { State = IterationState.Planned },
                        tx);

                    return false;
                }
                else
                {   //  Go up
                    return ClearOneSide(lastPlanningPartition, false, tx);
                }
            }
        }

        private async Task<bool> LoadBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationKey iterationKey,
            PlanningPartitionRecord planningPartition,
            CancellationToken ct)
        {
            var partitionCount =
                (int)Math.Ceiling((double)planningPartition.RecordCount / MAX_ROW_COUNT_PER_BLOCK);
            var protoBlocks = await LoadProtoBlocksAsync(
                queryClient,
                activity,
                iterationKey,
                planningPartition.MinIngestionTime,
                planningPartition.MaxIngestionTime,
                partitionCount,
                ct);

            using (var tx = Database.CreateTransaction())
            {
                if (protoBlocks.Count > 0)
                {
                    //  Refresh iteration entity
                    var iteration = Database.Iterations.Query(tx)
                        .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                        .First();

                    var nextBlockId = iteration.NextBlockId;

                    foreach (var protoBlock in protoBlocks)
                    {
                        Database.Blocks.AppendRecord(
                            new BlockRecord(
                                BlockState.Planned,
                                new BlockKey(iteration.IterationKey, nextBlockId++),
                                protoBlock.MinIngestionTime,
                                protoBlock.MaxIngestionTime,
                                protoBlock.CreationTime,
                                protoBlock.RecordCount,
                                0,
                                string.Empty,
                                string.Empty),
                            tx);
                    }
                    Database.Iterations.UpdateRecord(
                        iteration,
                        iteration with { NextBlockId = nextBlockId },
                        tx);
                }

                var result = ClearOneSide(planningPartition, true, tx);

                tx.Complete();

                return result;
            }
        }

        private async Task<IReadOnlyCollection<ProtoBlock>> LoadProtoBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationKey iterationKey,
            string minIngestionTime,
            string maxIngestionTime,
            int partitionCount,
            CancellationToken ct)
        {
            var iteration = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                .First();
            var rawProtoBlocks = await queryClient.GetProtoBlocksAsync(
                new KustoPriority(iterationKey),
                activity.GetSourceTableIdentity().TableName,
                activity.KqlQuery,
                iteration.CursorStart,
                iteration.CursorEnd,
                minIngestionTime,
                maxIngestionTime,
                partitionCount,
                ct);
            //  We are going to go through the list and subdivide protoblocks that are too big
            var normalizeProtoBlocks = new List<ProtoBlock>();

            foreach (var protoBlock in rawProtoBlocks)
            {
                if (protoBlock.RecordCount <= 2 * MAX_ROW_COUNT_PER_BLOCK)
                {
                    normalizeProtoBlocks.Add(protoBlock);
                }
                else
                {
                    var subProtoBlocks = await LoadProtoBlocksAsync(
                        queryClient,
                        activity,
                        iterationKey,
                        protoBlock.MinIngestionTime,
                        protoBlock.MaxIngestionTime,
                        (int)Math.Ceiling((double)protoBlock.RecordCount / MAX_ROW_COUNT_PER_BLOCK),
                        ct);

                    normalizeProtoBlocks.AddRange(subProtoBlocks);
                }
            }

            return normalizeProtoBlocks;
        }
    }
}