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
            if (tableRowItem.State == TableState.Planned)
            {
                tableRowItem = await PrepareTempTableAsync(tableRowItem, ct);
            }
            if (tableRowItem.State == TableState.TempTableCreating)
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

            iterationItem = iterationItem.ChangeState(TableState.TempTableCreated);

            var rowItemAppend = await RowItemGateway.AppendAsync(iterationItem, ct);

            //  We want to make sure this is recorded before we start ingesting data into it
            await rowItemAppend.ItemAppendTask;

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

            iterationItem = iterationItem.ChangeState(TableState.TempTableCreating);
            iterationItem.TempTableName = tempTableName;

            var rowItemAppend = await RowItemGateway.AppendAsync(iterationItem, ct);

            //  We want to ensure the item is appended before creating a temp table so
            //  we don't lose track of the table
            await rowItemAppend.ItemAppendTask;

            return iterationItem;
        }
    }
}