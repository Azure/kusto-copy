using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestration
{
    /// <summary>
    /// This orchestration is responsible for table's planning, i.e. creation
    /// blocks.
    /// </summary>
    internal class SourceTablePlanningOrchestration : SubOrchestrationBase
    {
        private const long MAX_RECORDS_PER_BLOCK = 1048576;

        public SourceTablePlanningOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        protected override async Task OnStartProcessAsync(CancellationToken ct)
        {
            var planningTableIterations = RowItemGateway.InMemoryCache
                .SourceTableMap
                .Values
                .Select(t => t.IterationMap.Values)
                .SelectMany(i => i)
                .Select(i => i.RowItem)
                .Where(i => i.State == SourceTableState.Planning);

            foreach (var i in planningTableIterations)
            {
                BackgroundTaskContainer.AddTask(OnPlanningIterationAsync(i, ct));
            }

            await Task.CompletedTask;
        }

        protected override void OnProcessRowItemAppended(RowItemAppend e, CancellationToken ct)
        {
            base.OnProcessRowItemAppended(e, ct);

            if (e.Item is SourceTableRowItem st
                && st.State == SourceTableState.Planning)
            {
                BackgroundTaskContainer.AddTask(
                    OnPlanningIterationAsync((SourceTableRowItem)e.Item, ct));
            }
        }

        private async Task OnPlanningIterationAsync(
            SourceTableRowItem iterationItem,
            CancellationToken ct)
        {
            var tableIdentity = iterationItem.SourceTable;
            var tableCache = RowItemGateway.InMemoryCache.SourceTableMap[tableIdentity];
            var iterationCache = tableCache.IterationMap[iterationItem.IterationId];
            var lastBlock = iterationCache.BlockMap
                .Values
                .OrderBy(b => b.RowItem.BlockId)
                .LastOrDefault();
            var lastBlockItem = lastBlock?.RowItem;
            var ingestionTimeStart = lastBlock == null
                ? string.Empty
                : lastBlock.RowItem.IngestionTimeEnd;
            var queryClient = DbClientFactory.GetDbQueryClient(
                tableIdentity.ClusterUri,
                tableIdentity.DatabaseName);
            var timeResolutionInSeconds = 100000;

            while (true)
            {
                var timeResolution = TimeSpan.FromSeconds(timeResolutionInSeconds);
                var distributions = await queryClient.GetRecordDistributionAsync(
                    iterationItem.IterationId,
                    tableIdentity.TableName,
                    iterationItem.CursorStart,
                    iterationItem.CursorEnd,
                    ingestionTimeStart,
                    timeResolution,
                    ct);

                if (distributions.Any())
                {
                    if (distributions[0].Cardinality < MAX_RECORDS_PER_BLOCK
                        || string.IsNullOrWhiteSpace(distributions[0].IngestionTimeStart)
                        || timeResolutionInSeconds == 1)
                    {
                        var aggregatedDistributions = AggregateDistributions(
                            distributions,
                            timeResolutionInSeconds != 1);

                        foreach (var distribution in aggregatedDistributions)
                        {
                            var newBlockItem = new SourceBlockRowItem
                            {
                                State = SourceBlockState.Planned,
                                SourceTable = tableIdentity,
                                IterationId = iterationItem.IterationId,
                                BlockId = lastBlockItem == null ? 0 : lastBlockItem.BlockId + 1,
                                IngestionTimeStart = distribution.IngestionTimeStart,
                                IngestionTimeEnd = distribution.IngestionTimeEnd
                            };

                            lastBlockItem = newBlockItem;
                            await RowItemGateway.AppendAsync(newBlockItem, ct);
                        }
                        await OnPlanningIterationAsync(iterationItem, ct);

                        return;
                    }
                    else
                    {   //  Try again with a lower resolution
                        timeResolutionInSeconds /= 10;
                    }
                }
                else
                {   //  No more records:  mark table iteration as "Planned"
                    var newIterationItem = iterationItem.ChangeState(SourceTableState.Planned);

                    await RowItemGateway.AppendAsync(newIterationItem, ct);

                    return;
                }
            }
        }

        private IEnumerable<RecordDistribution> AggregateDistributions(
            IEnumerable<RecordDistribution> distribution,
            bool canHaveBetterResolution)
        {
            var isNewItem = true;
            var ingestionTimeStart = string.Empty;
            var ingestionTimeEnd = string.Empty;
            var cardinality = (long)0;

            foreach (var item in distribution)
            {
                if (!isNewItem)
                {
                    if (cardinality + item.Cardinality > MAX_RECORDS_PER_BLOCK)
                    {   //  Seal previous items
                        yield return new RecordDistribution(
                            ingestionTimeStart,
                            ingestionTimeEnd,
                            cardinality);
                        isNewItem = true;
                    }
                    else
                    {
                        ingestionTimeEnd = item.IngestionTimeEnd;
                        cardinality += item.Cardinality;
                    }
                }
                if (isNewItem)
                {
                    if (item.Cardinality < MAX_RECORDS_PER_BLOCK || !canHaveBetterResolution)
                    {
                        ingestionTimeStart = item.IngestionTimeStart;
                        ingestionTimeEnd = item.IngestionTimeEnd;
                        cardinality = item.Cardinality;
                        isNewItem = false;
                    }
                    else
                    {   //  Need finer resolution
                        break;
                    }
                }
            }
            if (!isNewItem)
            {   //  Seal last items
                yield return new RecordDistribution(
                    ingestionTimeStart,
                    ingestionTimeEnd,
                    cardinality);
            }
        }
    }
}