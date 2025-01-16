using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class TempTableCreatingRunner : RunnerBase
    {
        public TempTableCreatingRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        /// <summary>
        /// Creates a temp table if it isn't already created.
        /// </summary>
        /// <param name="tableRowItem"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task<IterationRowItem> RunAsync(IterationRowItem tableRowItem, CancellationToken ct)
        {
            if (tableRowItem.State == IterationState.Planned)
            {
                tableRowItem = await PrepareTempTableAsync(tableRowItem, ct);
            }
            if (tableRowItem.State == IterationState.TempTableCreating)
            {
                tableRowItem = await CreateTempTableAsync(tableRowItem, ct);
            }

            return tableRowItem;
        }

        private async Task<IterationRowItem> CreateTempTableAsync(
            IterationRowItem iterationItem,
            CancellationToken ct)
        {
            var activity = RowItemGateway.InMemoryCache
                .ActivityMap[iterationItem.ActivityName]
                .RowItem;
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                activity.DestinationTable.ClusterUri,
                activity.DestinationTable.DatabaseName);
            var priority = new KustoPriority(
                iterationItem.ActivityName, iterationItem.IterationId);

            await dbCommandClient.DropTableIfExistsAsync(
                priority,
                iterationItem.TempTableName,
                ct);
            await dbCommandClient.CreateTempTableAsync(
                priority,
                activity.DestinationTable.TableName,
                iterationItem.TempTableName,
                ct);

            iterationItem = iterationItem.ChangeState(IterationState.TempTableCreated);
            RowItemGateway.Append(iterationItem);

            return iterationItem;
        }

        private async Task<IterationRowItem> PrepareTempTableAsync(
            IterationRowItem iterationItem,
            CancellationToken ct)
        {
            var activity = RowItemGateway.InMemoryCache
                .ActivityMap[iterationItem.ActivityName]
                .RowItem;
            var tempTableName =
                $"kc-{activity.DestinationTable.TableName}-{Guid.NewGuid().ToString("N")}";

            iterationItem = iterationItem.ChangeState(IterationState.TempTableCreating);
            iterationItem.TempTableName = tempTableName;

            //  We want to ensure the item is appended before creating a temp table so
            //  we don't lose track of the table
            await RowItemGateway.AppendAndPersistAsync(iterationItem, ct);

            return iterationItem;
        }
    }
}