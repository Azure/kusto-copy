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
    internal class SourcePlanningRunner : RunnerBase
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

        public SourcePlanningRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task RunAsync(
            SourceTableRowItem sourceTableRowItem,
            IImmutableDictionary<TableIdentity, Task> tempTableMap,
            CancellationToken ct)
        {
            await using (var planningProgress = CreatePlanningProgressBar(sourceTableRowItem))
            await using (var exportingProgress = CreateExportingProgressBar(sourceTableRowItem))
            {
                if (sourceTableRowItem.State == SourceTableState.Planning
                    || sourceTableRowItem.State == SourceTableState.Planned)
                {
                    var exportingRunner = new SourceExportingRunner(
                        Parameterization,
                        RowItemGateway,
                        DbClientFactory);
                    var blockMap = RowItemGateway.InMemoryCache
                        .SourceTableMap[sourceTableRowItem.SourceTable]
                        .IterationMap[sourceTableRowItem.IterationId]
                        .BlockMap;
                    var blobPathProvider = GetBlobPathFactory(sourceTableRowItem.SourceTable);
                    var exportingTasks = blockMap.Values
                        .Select(b => exportingRunner.RunAsync(
                            blobPathProvider,
                            tempTableMap,
                            sourceTableRowItem,
                            b.RowItem.BlockId,
                            b.RowItem.IngestionTimeStart,
                            b.RowItem.IngestionTimeEnd,
                            ct))
                        .ToImmutableArray();

                    //  Complete planning
                    if (sourceTableRowItem.State == SourceTableState.Planning)
                    {
                        var lastBlockItem = blockMap.Any()
                            ? blockMap.Values.Select(i => i.RowItem).ArgMax(b => b.BlockId)
                            : null;
                        var newTasks = await PlanNewBlocksAsync(
                            tempTableMap,
                            exportingRunner,
                            blobPathProvider,
                            sourceTableRowItem,
                            (lastBlockItem?.BlockId ?? 0) + 1,
                            lastBlockItem?.IngestionTimeEnd,
                            ct);

                        exportingTasks = exportingTasks.AddRange(newTasks);
                        sourceTableRowItem = sourceTableRowItem.ChangeState(SourceTableState.Planned);
                        await RowItemGateway.AppendAsync(sourceTableRowItem, ct);
                    }
                    await Task.WhenAll(exportingTasks);
                    sourceTableRowItem = sourceTableRowItem.ChangeState(SourceTableState.Exported);
                    await RowItemGateway.AppendAsync(sourceTableRowItem, ct);
                }
            }
        }

        #region Progress bars
        private ProgressBar CreatePlanningProgressBar(SourceTableRowItem sourceTableRowItem)
        {
            return new ProgressBar(
                TimeSpan.FromSeconds(5),
                () =>
                {
                    var iteration = RowItemGateway.InMemoryCache
                    .SourceTableMap[sourceTableRowItem.SourceTable]
                    .IterationMap[sourceTableRowItem.IterationId];
                    var iterationItem = iteration
                    .RowItem;
                    var blockMap = iteration
                    .BlockMap;

                    if (iterationItem.State == SourceTableState.Planning)
                    {
                        return new ProgressReport(
                            ProgessStatus.Progress,
                            $"Planned:  {sourceTableRowItem.SourceTable.ToStringCompact()}" +
                            $"({sourceTableRowItem.IterationId}) {blockMap.Count}");
                    }
                    else
                    {
                        return new ProgressReport(ProgessStatus.Completed, string.Empty);
                    }
                });
        }

        private ProgressBar CreateExportingProgressBar(
            SourceTableRowItem sourceTableRowItem)
        {
            return new ProgressBar(
                TimeSpan.FromSeconds(10),
                () =>
                {
                    var iteration = RowItemGateway.InMemoryCache
                    .SourceTableMap[sourceTableRowItem.SourceTable]
                    .IterationMap[sourceTableRowItem.IterationId];
                    var iterationItem = iteration
                    .RowItem;

                    if (iterationItem.State == SourceTableState.Planning)
                    {
                        return new ProgressReport(ProgessStatus.Nothing, string.Empty);
                    }
                    else if (iterationItem.State == SourceTableState.Planned)
                    {
                        var blockMap = iteration
                        .BlockMap;
                        var exportedCount = blockMap.Values
                        .Where(b => b.RowItem.State == SourceBlockState.Exported)
                        .Count();

                        return new ProgressReport(
                            ProgessStatus.Progress,
                            $"Exported:  {sourceTableRowItem.SourceTable.ToStringCompact()}" +
                            $"({sourceTableRowItem.IterationId}) {exportedCount}/{blockMap.Count}");
                    }
                    else
                    {
                        return new ProgressReport(ProgessStatus.Completed, string.Empty);
                    }
                });
        }
        #endregion

        private IBlobPathProvider GetBlobPathFactory(TableIdentity sourceTable)
        {
            var activity = Parameterization.Activities
                .Where(a => a.Source.GetTableIdentity() == sourceTable)
                .FirstOrDefault();

            if (activity == null)
            {
                throw new InvalidDataException($"Can't find table in parameters:  {sourceTable}");
            }
            else if (activity.Destinations.Count == 1)
            {
                var destinationTable = activity.Destinations.First().GetTableIdentity();
                var tempUriProvider = new TempUriProvider(DbClientFactory.GetDmCommandClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName));

                return tempUriProvider;
            }
            else
            {
                throw new NotImplementedException(
                    "Only single destination without storage account is supported");
            }
        }

        private async Task<IEnumerable<Task>> PlanNewBlocksAsync(
            IImmutableDictionary<TableIdentity, Task> tempTableMap,
            SourceExportingRunner exportingRunner,
            IBlobPathProvider blobPathProvider,
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
                    blobPathProvider,
                    tempTableMap,
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
            IBlobPathProvider blobPathProvider,
            IImmutableDictionary<TableIdentity, Task> tempTableMap,
            SourceExportingRunner exportingRunner,
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
                            blobPathProvider,
                            tempTableMap,
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
                        blobPathProvider,
                        tempTableMap,
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