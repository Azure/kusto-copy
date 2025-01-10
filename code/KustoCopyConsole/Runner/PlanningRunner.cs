using Azure.Storage.Blobs.Models;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class PlanningRunner : RunnerBase
    {
        #region Inner Types
        private record RecordDistributionInExtent(
            DateTime IngestionTime,
            string ExtentId,
            long RowCount,
            DateTime? MinCreatedOn);

        private record BatchExportBlock(
            IEnumerable<Task> exportingTasks,
            long nextBlockId,
            DateTime? nextIngestionTimeStart);
        #endregion

        private const int MAX_STATS_COUNT = 1000;
        private const long RECORDS_PER_BLOCK = 1048576;

        public PlanningRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task RunAsync(
            TableRowItem tableRowItem,
            Task tempTableTask,
            CancellationToken ct)
        {
            await using (var planningProgress = CreatePlanningProgressBar(tableRowItem))
            await using (var exportingProgress =
                CreateBlockStateProgressBar(tableRowItem, BlockState.Exporting))
            await using (var queuingProgress =
                CreateBlockStateProgressBar(tableRowItem, BlockState.Queued))
            await using (var ingestingProgress =
                CreateBlockStateProgressBar(tableRowItem, BlockState.Ingested))
            await using (var movingProgress =
                CreateBlockStateProgressBar(tableRowItem, BlockState.ExtentMoved))
            {
                var tempTableCreatingRunner = new TempTableCreatingRunner(
                    Parameterization,
                    RowItemGateway,
                    DbClientFactory);

                if (tableRowItem.State == TableState.Planning)
                {
                    tableRowItem = await PlanBlocksAsync(tableRowItem, ct);
                }
                await tempTableCreatingRunner.RunAsync(tableRowItem, ct);
            }
        }

        #region Progress bars
        private ProgressBar CreatePlanningProgressBar(TableRowItem tableRowItem)
        {
            return new ProgressBar(
                TimeSpan.FromSeconds(5),
                () =>
                {
                    var iteration = RowItemGateway.InMemoryCache
                    .SourceTableMap[tableRowItem.SourceTable]
                    .IterationMap[tableRowItem.IterationId];
                    var iterationItem = iteration
                    .RowItem;
                    var blockMap = iteration
                    .BlockMap;

                    return new ProgressReport(
                        iterationItem.State == TableState.Planning
                        ? ProgessStatus.Progress
                        : ProgessStatus.Completed,
                        $"Planned:  {tableRowItem.SourceTable.ToStringCompact()}"
                        + $"({tableRowItem.IterationId}) {blockMap.Count}");
                });
        }

        private ProgressBar CreateBlockStateProgressBar(
            TableRowItem tableRowItem,
            BlockState state)
        {
            return new ProgressBar(
                TimeSpan.FromSeconds(10),
                () =>
                {
                    var iteration = RowItemGateway.InMemoryCache
                    .SourceTableMap[tableRowItem.SourceTable]
                    .IterationMap[tableRowItem.IterationId];
                    var iterationItem = iteration
                    .RowItem;

                    if (iterationItem.State == TableState.Planning)
                    {
                        return new ProgressReport(ProgessStatus.Nothing, string.Empty);
                    }
                    else
                    {
                        var blockMap = iteration.BlockMap;
                        var stateReachedCount = blockMap.Values
                        .Where(b => b.RowItem.State >= state)
                        .Count();

                        return new ProgressReport(
                            stateReachedCount != blockMap.Count
                            ? ProgessStatus.Progress
                            : ProgessStatus.Completed,
                            $"{state}:  {tableRowItem.SourceTable.ToStringCompact()}" +
                            $"({tableRowItem.IterationId}) {stateReachedCount}/{blockMap.Count}");
                    }
                });
        }
        #endregion

        private async Task<TableRowItem> PlanBlocksAsync(
            TableRowItem tableItem,
            CancellationToken ct)
        {
            var queryClient = DbClientFactory.GetDbQueryClient(
                tableItem.SourceTable.ClusterUri,
                tableItem.SourceTable.DatabaseName);
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                tableItem.SourceTable.ClusterUri,
                tableItem.SourceTable.DatabaseName);

            //  Loop on block batches
            while (tableItem.State == TableState.Planning)
            {
                var blockMap = RowItemGateway.InMemoryCache
                    .SourceTableMap[tableItem.SourceTable]
                    .IterationMap[tableItem.IterationId]
                    .BlockMap;
                var lastBlock = blockMap.Any()
                    ? blockMap.Values.ArgMax(b => b.RowItem.BlockId).RowItem
                    : null;
                var distributionInExtents = await GetRecordDistributionInExtents(
                    tableItem,
                    lastBlock?.IngestionTimeEnd,
                    queryClient,
                    dbCommandClient,
                    ct);

                if (distributionInExtents.Any())
                {
                    var orderedDistributionInExtents = distributionInExtents
                        .OrderBy(d => d.IngestionTime)
                        .ThenBy(d => d.MinCreatedOn)
                        .ToImmutableArray();

                    while (orderedDistributionInExtents.Any())
                    {
                        (var newBlockItem, var remainingDistributionInExtents) = PlanSingleBlock(
                            tableItem,
                            lastBlock,
                            distributionInExtents);

                        orderedDistributionInExtents = remainingDistributionInExtents
                            .ToImmutableArray();
                        await RowItemGateway.AppendAsync(newBlockItem, ct);
                        lastBlock = newBlockItem;
                    }
                }
                else
                {
                    tableItem = tableItem.ChangeState(TableState.Planned);
                    await RowItemGateway.AppendAsync(tableItem, ct);
                }
            }

            return tableItem;
        }

        private (BlockRowItem, IEnumerable<RecordDistributionInExtent>) PlanSingleBlock(
            TableRowItem tableItem,
            BlockRowItem? lastBlock,
            IImmutableList<RecordDistributionInExtent> distributionInExtents)
        {
            var nextBlockId = (lastBlock?.BlockId ?? 0) + 1;
            var nextIngestionTimeStart = lastBlock?.IngestionTimeEnd;
            long cummulativeRowCount = 0;

            for (var i = 0; i != distributionInExtents.Count; ++i)
            {
                var distribution = distributionInExtents[i];

                cummulativeRowCount += distribution.RowCount;
                if (i + 1 == distributionInExtents.Count
                    || cummulativeRowCount + distributionInExtents[i + 1].RowCount > RECORDS_PER_BLOCK
                    || distribution.MinCreatedOn != distributionInExtents[i + 1].MinCreatedOn)
                {
                    var cummulativeDistributions = distributionInExtents.Take(i + 1);
                    var remainingDistributions = distributionInExtents.Skip(i + 1);
                    var blockItem = new BlockRowItem
                    {
                        State = BlockState.Planned,
                        SourceTable = tableItem.SourceTable,
                        DestinationTable = tableItem.DestinationTable,
                        IterationId = tableItem.IterationId,
                        BlockId = nextBlockId++,
                        IngestionTimeStart = cummulativeDistributions.Min(d => d.IngestionTime),
                        IngestionTimeEnd = cummulativeDistributions.Max(d => d.IngestionTime)
                    };

                    return (blockItem, remainingDistributions);
                }
            }

            throw new InvalidOperationException("We should never reach this code");
        }

        //  Merge results from query + show extents command
        private static async Task<IImmutableList<RecordDistributionInExtent>> GetRecordDistributionInExtents(
            TableRowItem sourceTableItem,
            DateTime? ingestionTimeStart,
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            CancellationToken ct)
        {
            var distributions = await queryClient.GetRecordDistributionAsync(
                sourceTableItem.IterationId,
                sourceTableItem.SourceTable.TableName,
                sourceTableItem.CursorStart,
                sourceTableItem.CursorEnd,
                ingestionTimeStart,
                MAX_STATS_COUNT,
                ct);

            if (distributions.Any())
            {
                var extentIds = distributions
                    .Select(d => d.ExtentId)
                    .Where(id => !string.IsNullOrWhiteSpace(id))
                    .Distinct();
                var extentDates = await dbCommandClient.GetExtentDatesAsync(
                    sourceTableItem.IterationId,
                    sourceTableItem.SourceTable.TableName,
                    extentIds,
                    ct);

                //  Check for racing condition where extents got merged and extent ids don't exist
                //  between 2 queries
                if (extentDates.Count == extentIds.Count())
                {
                    var distributionInExtents = distributions
                        .GroupJoin(
                        extentDates,
                        d => d.ExtentId, e => e.ExtentId,
                        (left, rightGroup) => new RecordDistributionInExtent(
                            left.IngestionTime,
                            left.ExtentId,
                            left.RowCount,
                            rightGroup.FirstOrDefault()?.MinCreatedOn))
                        .ToImmutableArray();

                    return distributionInExtents;
                }
                else
                {
                    return await GetRecordDistributionInExtents(
                        sourceTableItem,
                        ingestionTimeStart,
                        queryClient,
                        dbCommandClient,
                        ct);
                }
            }
            else
            {
                return ImmutableArray<RecordDistributionInExtent>.Empty;
            }
        }
    }
}