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

        private const int MAX_ACTIVE_BLOCKS_PER_ITERATION = 1200;
        private const int MIN_ACTIVE_BLOCKS_PER_ITERATION = 600;
        private const int MAX_ROW_COUNT_PER_BLOCK = 4000000;

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
            var activityParam = Parameterization.Activities[activityName];
            var source = activityParam.GetSourceTableIdentity();
            var destination = activityParam.GetDestinationTableIdentity();
            var queryClient = DbClientFactory.GetDbQueryClient(
                source.ClusterUri,
                source.DatabaseName);

            foreach (var iteration in iterations)
            {
                await RunIterationAsync(activityParam, iteration, queryClient, ct);
            }

            return iterations.Any();
        }

        private async Task RunIterationAsync(
            ActivityParameterization activityParam,
            IterationRecord iteration,
            DbQueryClient queryClient,
            CancellationToken ct)
        {
            if (iteration.State == IterationState.Starting)
            {
                var cursor = await queryClient.GetCurrentCursorAsync(
                    new KustoPriority(iteration.IterationKey),
                    ct);

                iteration = TransitionToPlanning(iteration, cursor);
            }
            if (iteration.State == IterationState.Planning && ShouldPlan(iteration.IterationKey))
            {
                await PlanBlocksAsync(queryClient, activityParam, iteration.IterationKey, ct);
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

        private IterationRecord TransitionToPlanning(IterationRecord iteration, string cursor)
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
            ActivityParameterization activityParam,
            IterationKey iterationKey,
            CancellationToken ct)
        {
            PlanningPartitionRecord? lastPlanningPartition = null;

            do
            {
                lastPlanningPartition = Database.PlanningPartitions.Query()
                    .Where(pf => pf.Equal(pp => pp.IterationKey, iterationKey))
                    .OrderByDescending(pp => pp.Level)
                    .ThenBy(pp => pp.PartitionId)
                    .Take(1)
                    .FirstOrDefault();
            }
            while (await PlanPartitionAsync(
                queryClient,
                activityParam,
                iterationKey,
                lastPlanningPartition,
                ct)
            && CanKeepPlanning(iterationKey));
        }

        private async Task<bool> PlanPartitionAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationKey iterationKey,
            PlanningPartitionRecord? lastPlanningPartition,
            CancellationToken ct)
        {
            if (lastPlanningPartition == null)
            {
                return await InitialPartitionAsync(
                    queryClient,
                    activityParam,
                    iterationKey,
                    ct);
            }
            else
            {
                return await SubPlanPartitionAsync(
                    queryClient,
                    activityParam,
                    lastPlanningPartition,
                    ct);
            }
        }

        private async Task<bool> InitialPartitionAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationKey iterationKey,
            CancellationToken ct)
        {
            var iteration = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                .First();
            var stats = await queryClient.GetRecordStatsAsync(
                new KustoPriority(iterationKey),
                activityParam.GetSourceTableIdentity().TableName,
                activityParam.KqlQuery,
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
                    0,
                    stats.RecordCount,
                    stats.MinIngestionTime,
                    stats.MedianIngestionTime,
                    stats.MaxIngestionTime);

                Database.PlanningPartitions.AppendRecord(planningPartition);

                return true;
            }
            else
            {
                ClearPlanning(iterationKey);

                return false;
            }
        }

        private async Task<bool> SubPlanPartitionAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            PlanningPartitionRecord parentPartition,
            CancellationToken ct)
        {
            void AppendPartition(
                PlanningPartitionRecord parentPartition,
                RecordStats stats,
                int partitionId,
                TransactionContext tx)
            {
                if (stats.RecordCount > 0)
                {
                    Database.PlanningPartitions.AppendRecord(
                        new PlanningPartitionRecord(
                            parentPartition.IterationKey,
                            parentPartition.Level + 1,
                            partitionId,
                            stats.RecordCount,
                            stats.MinIngestionTime,
                            stats.MedianIngestionTime,
                            stats.MaxIngestionTime),
                        tx);
                }
            }

            if (parentPartition.RecordCount > MAX_ROW_COUNT_BY_PARTITION)
            {   //  Sub partition
                var iteration = Database.Iterations.Query()
                    .Where(pf => pf.Equal(i => i.IterationKey, parentPartition.IterationKey))
                    .First();
                var statsLeftTask = queryClient.GetRecordStatsAsync(
                    new KustoPriority(parentPartition.IterationKey),
                    activityParam.GetSourceTableIdentity().TableName,
                    activityParam.KqlQuery,
                    iteration.CursorStart,
                    iteration.CursorEnd,
                    new DateTimeBoundary(parentPartition.MinIngestionTime, true),
                    //  Include median
                    new DateTimeBoundary(parentPartition.MedianIngestionTime, true),
                    ct);
                var statsRightTask = queryClient.GetRecordStatsAsync(
                    new KustoPriority(parentPartition.IterationKey),
                    activityParam.GetSourceTableIdentity().TableName,
                    activityParam.KqlQuery,
                    iteration.CursorStart,
                    iteration.CursorEnd,
                    //  Exclude median
                    new DateTimeBoundary(parentPartition.MedianIngestionTime, false),
                    new DateTimeBoundary(parentPartition.MaxIngestionTime, true),
                    ct);
                var statsLeft = await statsLeftTask;
                var statsRight = await statsRightTask;

                using (var tx = Database.CreateTransaction())
                {   //  Delete Parent
                    DeletePartition(parentPartition, tx);
                    //  Append children
                    AppendPartition(parentPartition, statsLeft, 0, tx);
                    AppendPartition(parentPartition, statsRight, 1, tx);

                    var result = ClearPlanning(parentPartition.IterationKey, tx);

                    tx.Complete();

                    return result;
                }
            }
            else
            {   //  Harvest blocks
                return await LoadBlocksAsync(
                    queryClient,
                    activityParam,
                    parentPartition,
                    ct);
            }
        }

        private void DeletePartition(
            PlanningPartitionRecord partition,
            TransactionContext tx)
        {
            Database.PlanningPartitions.Query(tx)
                .Where(pf => pf.Equal(pp => pp.IterationKey, partition.IterationKey))
                .Where(pf => pf.Equal(pp => pp.Level, partition.Level))
                .Where(pf => pf.Equal(pp => pp.PartitionId, partition.PartitionId))
                .Delete();
        }

        private async Task<bool> LoadBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            PlanningPartitionRecord parentPartition,
            CancellationToken ct)
        {
            var partitionCount =
                (int)Math.Ceiling((double)parentPartition.RecordCount / MAX_ROW_COUNT_PER_BLOCK);
            var protoBlocks = await LoadProtoBlocksAsync(
                queryClient,
                activityParam,
                parentPartition.IterationKey,
                parentPartition.MinIngestionTime,
                parentPartition.MaxIngestionTime,
                partitionCount,
                ct);

            using (var tx = Database.CreateTransaction())
            {
                if (protoBlocks.Count > 0)
                {
                    //  Refresh iteration entity
                    var iteration = Database.Iterations.Query(tx)
                        .Where(pf => pf.Equal(i => i.IterationKey, parentPartition.IterationKey))
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
                    DeletePartition(parentPartition, tx);
                    Database.Iterations.UpdateRecord(
                        iteration,
                        iteration with { NextBlockId = nextBlockId },
                        tx);
                }

                var result = ClearPlanning(parentPartition.IterationKey, tx);

                tx.Complete();

                return result;
            }
        }

        private async Task<IReadOnlyCollection<ProtoBlock>> LoadProtoBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
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
                activityParam.GetSourceTableIdentity().TableName,
                activityParam.KqlQuery,
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
                        activityParam,
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

        private bool ClearPlanning(IterationKey iterationKey, TransactionContext? tx = null)
        {
            var planning = Database.PlanningPartitions.Query(tx)
                .Take(1)
                .FirstOrDefault();

            if (planning == null)
            {   //  We cleared all partitions:  we're done
                var iteration = Database.Iterations.Query(tx)
                    .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                    .First();

                Database.Iterations.UpdateRecord(
                    iteration,
                    iteration with { State = IterationState.Planned },
                    tx);

                return false;
            }
            else
            {
                return true;
            }
        }
    }
}