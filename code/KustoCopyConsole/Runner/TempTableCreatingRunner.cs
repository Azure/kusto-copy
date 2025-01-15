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
            IterationRowItem tableRowItem,
            CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                tableRowItem.DestinationTable.ClusterUri,
                tableRowItem.DestinationTable.DatabaseName);

            await dbCommandClient.DropTableIfExistsAsync(
                tableRowItem.IterationId,
                tableRowItem.TempTableName,
                ct);
            await dbCommandClient.CreateTempTableAsync(
                tableRowItem.IterationId,
                tableRowItem.DestinationTable.TableName,
                tableRowItem.TempTableName,
                ct);

            tableRowItem = tableRowItem.ChangeState(TableState.TempTableCreated);

            var rowItemAppend = await RowItemGateway.AppendAsync(tableRowItem, ct);

            //  We want to make sure this is recorded before we start ingesting data into it
            await rowItemAppend.ItemAppendTask;
            return tableRowItem;
        }

        private async Task<IterationRowItem> PrepareTempTableAsync(
            IterationRowItem tableRowItem,
            CancellationToken ct)
        {
            var tempTableName =
                $"kc-{tableRowItem.DestinationTable.TableName}-{Guid.NewGuid().ToString("N")}";

            tableRowItem = tableRowItem.ChangeState(TableState.TempTableCreating);
            tableRowItem.TempTableName = tempTableName;

            var rowItemAppend = await RowItemGateway.AppendAsync(tableRowItem, ct);

            //  We want to ensure the item is appended before creating a temp table so
            //  we don't lose track of the table
            await rowItemAppend.ItemAppendTask;

            return tableRowItem;
        }
    }
}