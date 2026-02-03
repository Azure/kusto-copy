using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Linq;

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

        private const int MAX_ACTIVE_BLOCKS_PER_ITERATION = 1000;
        private const int MIN_ACTIVE_BLOCKS_PER_ITERATION = 500;
        private const int MAX_ROW_COUNT_PER_BLOCK = 4000000;
        private const int MAX_ROW_COUNT_BY_PARTITION = 64 * MAX_ROW_COUNT_PER_BLOCK;

        public PlanningRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(5))
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
                await PlanBlocksAsync(queryClient, activity, iteration, ct);
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
            IterationRecord iteration,
            CancellationToken ct)
        {
            var minIngestionTime = iteration.LastBlockEndIngestionTime == null
                ? null
                : new DateTimeBoundary(iteration.LastBlockEndIngestionTime, false);

            if (await PlanPartitionBlocksAsync(
                1,
                queryClient,
                activity,
                iteration,
                minIngestionTime,
                null,
                ct))
            {
                using (var tx = Database.CreateTransaction())
                {
                    var refreshedIteration = Database.Iterations.Query(tx)
                        .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                        .First();

                    Database.Iterations.UpdateRecord(
                        refreshedIteration,
                        refreshedIteration with { State = IterationState.Planned },
                        tx);

                    tx.Complete();
                }
            }
        }

        private async Task<bool> PlanPartitionBlocksAsync(
            int generation,
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iteration,
            DateTimeBoundary? minIngestionTime,
            DateTimeBoundary? maxIngestionTime,
            CancellationToken ct)
        {
            if (CanKeepPlanning(iteration.IterationKey))
            {
                var stats = await queryClient.GetRecordStatsAsync(
                    new KustoPriority(iteration.IterationKey),
                    activity.GetSourceTableIdentity().TableName,
                    activity.KqlQuery,
                    iteration.CursorStart,
                    iteration.CursorEnd,
                    minIngestionTime,
                    maxIngestionTime,
                    ct);

                //  Maybe we found an empty partition
                if (stats.RecordCount > 0)
                {
                    if (stats.RecordCount > MAX_ROW_COUNT_BY_PARTITION)
                    {   //  Split partition
                        //  We include the median in the left half
                        return await PlanPartitionBlocksAsync(
                            generation + 1,
                            queryClient,
                            activity,
                            iteration,
                            new DateTimeBoundary(stats.MinIngestionTime, true),
                            new DateTimeBoundary(stats.MedianIngestionTime, true),
                            ct)
                            &&
                            //  We exclude the median in the right half
                            await PlanPartitionBlocksAsync(
                                generation + 1,
                                queryClient,
                                activity,
                                iteration,
                                new DateTimeBoundary(stats.MedianIngestionTime, false),
                                new DateTimeBoundary(stats.MaxIngestionTime, true),
                                ct);
                    }
                    else
                    {
                        await LoadBlocksAsync(
                            queryClient,
                            activity,
                            iteration,
                            stats.MinIngestionTime,
                            stats.MaxIngestionTime,
                            (int)Math.Ceiling((double)stats.RecordCount / MAX_ROW_COUNT_PER_BLOCK),
                            ct);

                        return true;
                    }
                }
                else
                {   //  No record found
                    return true;
                }
            }
            else
            {   //  Couldn't keep planning:  we aren't done 
                return false;
            }
        }

        private async Task LoadBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iteration,
            string minIngestionTime,
            string maxIngestionTime,
            int partitionCount,
            CancellationToken ct)
        {
            var protoBlocks = await LoadProtoBlocksAsync(
                queryClient,
                activity,
                iteration,
                minIngestionTime,
                maxIngestionTime,
                partitionCount,
                ct);

            if (protoBlocks.Count > 0)
            {
                using (var tx = Database.CreateTransaction())
                {
                    //  Refresh iteration entity
                    iteration = Database.Iterations.Query(tx)
                        .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
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
                        iteration with
                        {
                            NextBlockId = nextBlockId,
                            LastBlockEndIngestionTime = protoBlocks.Last().MaxIngestionTime
                        },
                        tx);

                    tx.Complete();
                }
            }
        }

        private async Task<IReadOnlyCollection<ProtoBlock>> LoadProtoBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iteration,
            string minIngestionTime,
            string maxIngestionTime,
            int partitionCount,
            CancellationToken ct)
        {
            var rawProtoBlocks = await queryClient.GetProtoBlocksAsync(
                new KustoPriority(iteration.IterationKey),
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
                        iteration,
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