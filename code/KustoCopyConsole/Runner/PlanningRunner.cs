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

        private const int MAX_PROTO_BLOCK_COUNT = 10000;

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

        private async Task PlanIterationAsync(
            IterationRecord iteration,
            CancellationToken ct)
        {
            var activity = Parameterization.Activities[iteration.IterationKey.ActivityName];
            var source = activity.GetSourceTableIdentity();
            var destination = activity.GetDestinationTableIdentity();
            var queryClient = DbClientFactory.GetDbQueryClient(
                source.ClusterUri,
                source.DatabaseName);
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                source.ClusterUri,
                source.DatabaseName);

            if (iteration.State == IterationState.Starting)
            {
                var cursor = await queryClient.GetCurrentCursorAsync(
                    new KustoPriority(iteration.IterationKey),
                    ct);

                iteration = PlanningIteration(iteration, cursor);
            }
            if (iteration.State == IterationState.Planning)
            {
                await PlanBlocksAsync(queryClient, activity, iteration, ct);
                Database.Iterations.UpdateRecord(
                    iteration,
                    iteration with { State = IterationState.Planned });
            }
        }

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
            var lastBlock = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iteration.IterationKey))
                .FirstOrDefault();

            do
            {
                var protoBlocks = await LoadProtoBlocksAsync(
                    queryClient, activity, iteration, lastBlock?.IngestionTimeEnd, ct);

                if (!protoBlocks.Any())
                {
                    lastBlock = null;
                }
                else
                {
                    var blocks = CreateBlocks(protoBlocks, iteration.IterationKey, lastBlock)
                        .ToImmutableArray();

                    Database.Blocks.AppendRecords(blocks);
                    lastBlock = blocks.Last();
                }
            }
            while (lastBlock != null);
        }

        private static async Task<IEnumerable<ProtoBlock>> LoadProtoBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iteration,
            string? ingestionTimeStart,
            CancellationToken ct)
        {
            var protoBlocks = await queryClient.GetExtentStatsAsync(
                new KustoPriority(iteration.IterationKey),
                activity.GetSourceTableIdentity().TableName,
                iteration.CursorStart,
                iteration.CursorStart == null ? null : iteration.CursorEnd,
                ingestionTimeStart,
                MAX_PROTO_BLOCK_COUNT,
                ct);

            //  We test for racing conditions:
            //  the extents were changed between query and .show extents
            if (protoBlocks.Any(e => e.CreationTime == null))
            {
                return await LoadProtoBlocksAsync(
                    queryClient, activity, iteration, ingestionTimeStart, ct);
            }
            else
            {
                return protoBlocks;
            }
        }

        private IEnumerable<BlockRecord> CreateBlocks(
            IEnumerable<ProtoBlock> protoBlocks,
            IterationKey iterationKey,
            BlockRecord? lastBlock)
        {
            foreach (var current in protoBlocks)
            {
                var blockId = lastBlock == null
                    ? 0
                    : lastBlock.BlockKey.BlockId + 1;
                var block = new BlockRecord(
                    BlockState.Planned,
                    new BlockKey(iterationKey, blockId),
                    blockId == 0 ? null : current.StartIngestionTime,
                    current.EndIngestionTime,
                    current.CreationTime!.Value,
                    0,
                    string.Empty,
                    string.Empty);

                yield return block;
                lastBlock = block;
            }
        }
    }
}