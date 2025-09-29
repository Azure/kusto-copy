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
        private const long RECORDS_PER_BLOCK = 8 * 1048576;

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

                iteration = StartIteration(iteration, cursor);
            }
            await ValidateIngestionTimeAsync(queryClient, activity, iteration, ct);
            if (iteration.State == IterationState.Planning)
            {
                await PlanBlocksAsync(queryClient, activity, iteration, ct);
            }
        }

        private IterationRecord StartIteration(IterationRecord iteration, string cursor)
        {
            using (var tx = Database.Database.CreateTransaction())
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

        private async Task ValidateIngestionTimeAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iterationRecord,
            CancellationToken ct)
        {
            var hasNullIngestionTime = await queryClient.HasNullIngestionTime(
                new KustoPriority(iterationRecord.IterationKey),
                activity.GetSourceTableIdentity().TableName,
                activity.KqlQuery,
                ct);

            if (hasNullIngestionTime)
            {
                throw new CopyException(
                    $"Iteration {iterationRecord.IterationKey}:  " +
                    $"null ingestion time are present.  Null ingestion time aren't supported.",
                    false);
            }
        }

        private async Task PlanBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iteration,
            CancellationToken ct)
        {
            var ingestionTimeInterval = await queryClient.GetIngestionTimeIntervalAsync(
                new KustoPriority(iteration.IterationKey),
                activity.GetSourceTableIdentity().TableName,
                activity.KqlQuery,
                iteration.CursorStart,
                iteration.CursorEnd,
                ct);

            if (string.IsNullOrWhiteSpace(ingestionTimeInterval.MinIngestionTime)
                || string.IsNullOrWhiteSpace(ingestionTimeInterval.MaxIngestionTime))
            {   //  No ingestion time:  either no rows or no rows with ingestion time
                Database.Iterations.UpdateRecord(
                    iteration,
                    iteration with
                    {
                        State = IterationState.Completed
                    });
            }
            else
            {
                await PlanBlocksAsync(
                    queryClient,
                    activity,
                    iteration,
                    ingestionTimeInterval,
                    ct);
            }
        }

        private async Task PlanBlocksAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iteration,
            IngestionTimeInterval ingestionTimeInterval,
            CancellationToken ct)
        {
            //  Do blocks one batch at the time until completion
            while (iteration.State == IterationState.Planning)
            {
                var lastBlock = Database.Blocks.Query()
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.IterationKey.ActivityName,
                        iteration.IterationKey.ActivityName))
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.IterationKey.IterationId,
                        iteration.IterationKey.IterationId))
                    .OrderByDesc(b => b.BlockKey.BlockId)
                    .Take(1)
                    .FirstOrDefault();
                var hasReachedUpperIngestionTime = await PlanBlocksBatchAsync(
                    activity,
                    iteration,
                    lastBlock == null ? 1 : lastBlock.BlockKey.BlockId + 1,
                    lastBlock?.IngestionTimeEnd.ToString(),
                    lastBlock?.IngestionTimeEnd.ToString() ?? ingestionTimeInterval.MinIngestionTime,
                    ingestionTimeInterval.MaxIngestionTime,
                    queryClient,
                    ct);

                if (hasReachedUpperIngestionTime)
                {
                    Database.Iterations.UpdateRecord(
                        iteration,
                        iteration with
                        {
                            State = IterationState.Planned
                        });

                    return;
                }
            }
        }

        private async Task<bool> PlanBlocksBatchAsync(
            ActivityParameterization activity,
            IterationRecord iteration,
            long nextBlockId,
            string? lastIngestionTime,
            string lowerIngestionTime,
            string upperIngestionTime,
            DbQueryClient queryClient,
            CancellationToken ct)
        {
            var distribution = await queryClient.GetRecordDistributionAsync(
                new KustoPriority(iteration.IterationKey),
                activity.GetSourceTableIdentity().TableName,
                activity.KqlQuery,
                iteration.CursorStart,
                iteration.CursorEnd,
                lastIngestionTime,
                lowerIngestionTime,
                upperIngestionTime,
                RECORDS_PER_BLOCK,
                ct);

            //  Check for racing condition where extents got merged and extent ids didn't exist
            //  when retrieving extent creation date
            if (distribution.RecordGroups.Any(d => d.MinCreatedOn == null))
            {
                return await PlanBlocksBatchAsync(
                    activity,
                    iteration,
                    nextBlockId,
                    lastIngestionTime,
                    lowerIngestionTime,
                    upperIngestionTime,
                    queryClient,
                    ct);
            }
            else if (distribution.RecordGroups.Any())
            {
                var blockRecords = distribution.RecordGroups
                    .Select(r => new BlockRecord(
                        BlockState.Planned,
                        new BlockKey(iteration.IterationKey, nextBlockId++),
                        r.IngestionTimeStart,
                        r.IngestionTimeEnd,
                        r.MinCreatedOn!.Value,
                        r.MaxCreatedOn!.Value,
                        r.RowCount,
                        0,
                        string.Empty,
                        string.Empty))
                    .ToImmutableArray();

                Database.Blocks.AppendRecords(blockRecords);
            }

            return distribution.HasReachedUpperIngestionTime;
        }
    }
}