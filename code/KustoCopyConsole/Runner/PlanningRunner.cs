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

        private const int MAX_ACTIVE_BLOCKS_PER_ITERATION = 2000;
        private const int MIN_ACTIVE_BLOCKS_PER_ITERATION = 1000;
        private const int MAX_ROW_COUNT_PER_BLOCK = 4000000;
        private const int MAX_ROW_COUNT_PER_PARTITION = 250 * MAX_ROW_COUNT_PER_BLOCK;

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

            return false;
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
                await PartitionDataAsync(queryClient, activityParam, iteration.IterationKey, ct);
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

        private async Task PartitionDataAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationKey iterationKey,
            CancellationToken ct)
        {
            PlanningPartitionRecord? lastPartition = null;

            do
            {
                lastPartition = Database.PlanningPartitions.Query()
                    .Where(pf => pf.Equal(pp => pp.IterationKey, iterationKey))
                    .OrderByDescending(pp => pp.Level)
                    .ThenBy(pp => pp.PartitionId)
                    .Take(1)
                    .FirstOrDefault();
            }
            while (await SubPartitionAsync(
                queryClient,
                activityParam,
                iterationKey,
                lastPartition,
                ct)
            && CanKeepPlanning(iterationKey));
        }

        private async Task<bool> SubPartitionAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationKey iterationKey,
            PlanningPartitionRecord? lastPartition,
            CancellationToken ct)
        {
            if (lastPartition == null
                || (lastPartition.Level <= 1 && lastPartition.RowCount > MAX_ROW_COUNT_PER_PARTITION))
            {
                return await PartitionRowsAsync(
                    queryClient,
                    activityParam,
                    iterationKey,
                    lastPartition,
                    ct);
            }
            else
            {
                return await LoadBlocksAsync(
                    queryClient,
                    activityParam,
                    lastPartition,
                    ct);
            }
        }

        private async Task<bool> PartitionRowsAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            IterationKey iterationKey,
            PlanningPartitionRecord? parentPartition,
            CancellationToken ct)
        {
            var iteration = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, iterationKey))
                .First();
            var rowPartitions = await queryClient.PartitionRowsAsync(
                new KustoPriority(iterationKey),
                activityParam.GetSourceTableIdentity().TableName,
                activityParam.KqlQuery,
                iteration.CursorStart,
                iteration.CursorEnd,
                parentPartition?.MinIngestionTime,
                parentPartition?.MaxIngestionTime,
                GetPartitionResolution(parentPartition?.Level),
                ct);

            if (rowPartitions.Count() > 0)
            {
                var mergedRowPartitions = Merge(rowPartitions);
                var planningPartitions = mergedRowPartitions
                    .Index()
                    .Select(rp => new PlanningPartitionRecord(
                        iteration.IterationKey,
                        (parentPartition?.Level ?? 0) + 1,
                        GetPartitionId(parentPartition?.Level, parentPartition?.PartitionId, rp.Index),
                        rp.Item.RowCount,
                        rp.Item.MinIngestionTime,
                        rp.Item.MaxIngestionTime));

                using (var tx = Database.CreateTransaction())
                {
                    Database.PlanningPartitions.AppendRecords(planningPartitions);
                    DeletePartition(parentPartition, tx);

                    tx.Complete();
                }

                return true;
            }
            else
            {
                ClearPlanning(iterationKey);

                return false;
            }
        }

        private void DeletePartition(
            PlanningPartitionRecord? partition,
            TransactionContext tx)
        {
            if (partition != null)
            {
                Database.PlanningPartitions.Query(tx)
                    .Where(pf => pf.Equal(pp => pp.IterationKey, partition.IterationKey))
                    .Where(pf => pf.Equal(pp => pp.Level, partition.Level))
                    .Where(pf => pf.Equal(pp => pp.PartitionId, partition.PartitionId))
                    .Delete();
            }
        }

        private IEnumerable<RowPartition> Merge(IEnumerable<RowPartition> rowPartitions)
        {
            var mergedRowPartitions = new List<RowPartition>(rowPartitions.Count());
            var bufferPartition = (RowPartition?)null;

            foreach (var partition in rowPartitions)
            {
                if (bufferPartition == null)
                {
                    bufferPartition = partition;
                }
                else if (bufferPartition.RowCount + partition.RowCount < MAX_ROW_COUNT_PER_PARTITION)
                {   //  Merge
                    bufferPartition = new RowPartition(
                        bufferPartition.RowCount + partition.RowCount,
                        bufferPartition.MinIngestionTime,
                        partition.MaxIngestionTime);
                }
                else
                {
                    mergedRowPartitions.Add(bufferPartition);
                    bufferPartition = partition;
                }
            }
            if (bufferPartition != null)
            {
                mergedRowPartitions.Add(bufferPartition);
            }

            return mergedRowPartitions;
        }

        private IEnumerable<ProtoBlock> Merge(IEnumerable<ProtoBlock> protoBlocks)
        {
            DateTime? Max(DateTime? a, DateTime? b)
            {
                return a == null && b == null
                    ? null
                    : a == null && b != null
                    ? b
                    : a != null && b == null
                    ? a
                    : a!.Value > b!.Value
                    ? a
                    : b;
            }

            var mergedProtoBlocks = new List<ProtoBlock>(protoBlocks.Count());
            var bufferProtoBlock = (ProtoBlock?)null;

            foreach (var protoBlock in protoBlocks)
            {
                if (bufferProtoBlock == null)
                {
                    bufferProtoBlock = protoBlock;
                }
                else if (bufferProtoBlock.RowCount + protoBlock.RowCount < MAX_ROW_COUNT_PER_BLOCK)
                {   //  Merge
                    bufferProtoBlock = new ProtoBlock(
                        bufferProtoBlock.RowCount + protoBlock.RowCount,
                        bufferProtoBlock.MinIngestionTime,
                        protoBlock.MaxIngestionTime,
                        Max(bufferProtoBlock.CreationTime, protoBlock.CreationTime));
                }
                else
                {
                    mergedProtoBlocks.Add(bufferProtoBlock);
                    bufferProtoBlock = protoBlock;
                }
            }
            if (bufferProtoBlock != null)
            {
                mergedProtoBlocks.Add(bufferProtoBlock);
            }

            return mergedProtoBlocks;
        }

        private TimeSpan GetPartitionResolution(int? level)
        {
            return level switch
            {
                null => TimeSpan.FromDays(1),
                1 => TimeSpan.FromMinutes(1),
                _ => throw new NotSupportedException($"Level {level}")
            };
        }

        private int GetPartitionId(int? level, int? parentPartitionId, int index)
        {
            return level switch
            {
                null => index,
                1 => (int)(GetPartitionResolution(null) / GetPartitionResolution(1))
                * parentPartitionId!.Value + index,
                _ => throw new NotSupportedException($"Level {level}")
            };
        }

        private async Task<bool> LoadBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            PlanningPartitionRecord parentPartition,
            CancellationToken ct)
        {
            var protoBlocks = await LoadProtoBlocksAsync(
                queryClient,
                activityParam,
                parentPartition,
                ct);

            using (var tx = Database.CreateTransaction())
            {
                DeletePartition(parentPartition, tx);
                if (protoBlocks.Count() > 0)
                {
                    //  Refresh iteration entity
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
                            string.Empty))
                        .ToImmutableArray();

                    Database.Blocks.AppendRecords(blocks, tx);
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

        private async Task<IEnumerable<ProtoBlock>> LoadProtoBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activityParam,
            PlanningPartitionRecord parentPartition,
            CancellationToken ct)
        {
            var iteration = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, parentPartition.IterationKey))
                .First();
            var rawProtoBlocks = await queryClient.GetProtoBlocksAsync(
                new KustoPriority(parentPartition.IterationKey),
                activityParam.GetSourceTableIdentity().TableName,
                activityParam.KqlQuery,
                iteration.CursorStart,
                iteration.CursorEnd,
                parentPartition.MinIngestionTime,
                parentPartition.MaxIngestionTime,
                TimeSpan.FromSeconds(0.01),
                ct);
            var mergedProtoBlocks = Merge(rawProtoBlocks);

            return mergedProtoBlocks;
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