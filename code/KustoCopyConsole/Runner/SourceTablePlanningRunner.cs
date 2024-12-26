using Azure.Data.Tables;
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
    internal class SourceTablePlanningRunner : RunnerBase
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

        public SourceTablePlanningRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task RunAsync(SourceTableRowItem sourceTableRowItem, CancellationToken ct)
        {
            if (sourceTableRowItem.State == SourceTableState.Planning)
            {
                var blockMap = RowItemGateway.InMemoryCache
                    .SourceTableMap[sourceTableRowItem.SourceTable]
                    .IterationMap[sourceTableRowItem.IterationId]
                    .BlockMap;
                var exportingRunner = new SourceTableExportingRunner(
                    Parameterization,
                    RowItemGateway,
                    DbClientFactory);
                var exportingTasks = blockMap.Values
                    .Select(b => exportingRunner.RunAsync(
                        sourceTableRowItem,
                        b.RowItem.BlockId,
                        b.RowItem.IngestionTimeStart,
                        b.RowItem.IngestionTimeEnd,
                        ct))
                    .ToImmutableArray();
                var lastBlockItem = blockMap.Any()
                    ? blockMap.Values.Select(i => i.RowItem).ArgMax(b => b.BlockId)
                    : null;
                var newTasks = await PlanNewBlocksAsync(
                    exportingRunner,
                    sourceTableRowItem,
                    (lastBlockItem?.BlockId ?? 0) + 1,
                    lastBlockItem?.IngestionTimeEnd,
                    ct);

                exportingTasks = exportingTasks.AddRange(newTasks);
                sourceTableRowItem = sourceTableRowItem.ChangeState(SourceTableState.Planned);
                await RowItemGateway.AppendAsync(sourceTableRowItem, ct);
            }
        }

        private async Task<IEnumerable<Task>> PlanNewBlocksAsync(
            SourceTableExportingRunner exportingRunner,
            SourceTableRowItem sourceTableItem,
            long nextBlockId,
            DateTime? nextIngestionTimeStart,
            CancellationToken ct)
        {
            var exportingTasks = ImmutableArray<Task>.Empty.ToBuilder();
            var queryClient = DbClientFactory.GetDbQueryClient(
                sourceTableItem.SourceTable.ClusterUri,
                sourceTableItem.SourceTable.DatabaseName);
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                sourceTableItem.SourceTable.ClusterUri,
                sourceTableItem.SourceTable.DatabaseName);

            //  Loop on block batch
            while (true)
            {
                var distributionInExtents = await GetRecordDistributionInExtents(
                    sourceTableItem,
                    nextIngestionTimeStart,
                    queryClient,
                    dbCommandClient,
                    ct);
                var newExportingTasks = PlanBlockBatch(
                    exportingRunner,
                    sourceTableItem,
                    ref nextBlockId,
                    ref nextIngestionTimeStart,
                    distributionInExtents,
                    ct);

                if (newExportingTasks.Any())
                {
                    exportingTasks.AddRange(newExportingTasks);
                }
                else
                {
                    return exportingTasks.ToImmutableArray();
                }
            }
        }

        private IEnumerable<Task> PlanBlockBatch(
            SourceTableExportingRunner exportingRunner,
            SourceTableRowItem sourceTableItem,
            ref long nextBlockId,
            ref DateTime? nextIngestionTimeStart,
            IImmutableList<RecordDistributionInExtent> distributionInExtents,
            CancellationToken ct)
        {
            var exportingTasks = new List<Task>();

            if (distributionInExtents.Any())
            {
                var orderedDistributionInExtents = distributionInExtents
                    .OrderBy(d => d.IngestionTime)
                    .ThenBy(d => d.MinCreatedOn);
                long cummulativeRowCount = 0;
                var cummulativeDistributions = new List<RecordDistributionInExtent>();
                var currentMinCreatedOn = distributionInExtents.First().MinCreatedOn;

                foreach (var distribution in orderedDistributionInExtents)
                {
                    if (cummulativeDistributions.Any()
                        && (cummulativeRowCount + distribution.RowCount > RECORDS_PER_BLOCK
                        || distribution.MinCreatedOn != currentMinCreatedOn))
                    {
                        exportingTasks.Add(exportingRunner.RunAsync(
                            sourceTableItem,
                            nextBlockId++,
                            cummulativeDistributions.Min(d => d.IngestionTime),
                            cummulativeDistributions.Max(d => d.IngestionTime),
                            ct));
                        nextIngestionTimeStart =
                            cummulativeDistributions.Max(d => d.IngestionTime);
                        cummulativeDistributions.Clear();
                        currentMinCreatedOn = distribution.MinCreatedOn;
                        cummulativeRowCount = distribution.RowCount;
                    }
                    else
                    {
                        cummulativeRowCount += distribution.RowCount;
                    }
                    cummulativeDistributions.Add(distribution);
                }
                if (distributionInExtents.Count() < MAX_STATS_COUNT
                    && cummulativeDistributions.Any())
                {
                    exportingTasks.Add(exportingRunner.RunAsync(
                        sourceTableItem,
                        nextBlockId++,
                        cummulativeDistributions.Min(d => d.IngestionTime),
                        cummulativeDistributions.Max(d => d.IngestionTime),
                        ct));
                    nextIngestionTimeStart =
                        cummulativeDistributions.Max(d => d.IngestionTime);
                }
            }

            return exportingTasks;
        }

        private static async Task<IImmutableList<RecordDistributionInExtent>> GetRecordDistributionInExtents(
            SourceTableRowItem sourceTableItem,
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