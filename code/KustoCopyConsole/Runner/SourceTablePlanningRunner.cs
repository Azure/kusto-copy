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
        #endregion

        private const long MAX_RECORDS_PER_BLOCK = 1048576;

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
                var lastBlockItem = blockMap.Any()
                    ? blockMap.Values.Select(i => i.RowItem).ArgMax(b => b.BlockId)
                    : null;

                while (true)
                {
                    //lastBlockItem =
                    await PlanNewBlockAsync(
                        sourceTableRowItem,
                        lastBlockItem,
                        ct);
                }
            }
        }

        private async Task PlanNewBlockAsync(
            SourceTableRowItem sourceTableItem,
            SourceBlockRowItem? lastBlockItem,
            CancellationToken ct)
        {
            var ingestionTimeStart = lastBlockItem == null
                ? string.Empty
                : lastBlockItem.IngestionTimeEnd;
            var queryClient = DbClientFactory.GetDbQueryClient(
                sourceTableItem.SourceTable.ClusterUri,
                sourceTableItem.SourceTable.DatabaseName);
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                sourceTableItem.SourceTable.ClusterUri,
                sourceTableItem.SourceTable.DatabaseName);
            var distributionInExtents = await GetRecordDistributionInExtents(
                sourceTableItem,
                ingestionTimeStart,
                queryClient,
                dbCommandClient,
                ct);

            if (distributionInExtents.Any())
            {
                var orderedDistributionInExtents = distributionInExtents
                    .OrderBy(d => d.IngestionTime)
                    .ThenBy(d => d.MinCreatedOn);
                var idealRowCount = 1048576;
                long cummulativeRowCount = 0;
                var cummulativeDistributions = new List<RecordDistributionInExtent>();
                var currentMinCreatedOn = distributionInExtents.First().MinCreatedOn;

                foreach (var distribution in orderedDistributionInExtents)
                {
                    if (cummulativeDistributions.Any()
                        && (cummulativeRowCount + distribution.RowCount > idealRowCount
                        || distribution.MinCreatedOn != currentMinCreatedOn))
                    {
                        cummulativeDistributions.Clear();
                        currentMinCreatedOn = distribution.MinCreatedOn;
                        cummulativeRowCount = distribution.RowCount;
                        //  Generate block
                    }
                    else
                    {
                        cummulativeRowCount += distribution.RowCount;
                    }
                    cummulativeDistributions.Add(distribution);
                }

                throw new NotImplementedException();
            }
        }

        private static async Task<IImmutableList<RecordDistributionInExtent>> GetRecordDistributionInExtents(
            SourceTableRowItem sourceTableItem,
            string ingestionTimeStart,
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