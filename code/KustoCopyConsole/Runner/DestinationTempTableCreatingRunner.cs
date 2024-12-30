using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class DestinationTempTableCreatingRunner : RunnerBase
    {
        public DestinationTempTableCreatingRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        /// <summary>
        /// Creates a temp table for the destination if it isn't already created.
        /// </summary>
        /// <param name="sourceTableRowItem"></param>
        /// <param name="destinationTable"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task RunAsync(
            SourceTableRowItem sourceTableRowItem,
            TableIdentity destinationTable,
            CancellationToken ct)
        {
            var destinationIteration = await EnsureDestinationIterationAsync(
                sourceTableRowItem,
                destinationTable,
                ct);

            if (destinationIteration.State == DestinationTableState.TempTableCreating)
            {
                var dbCommandClient = DbClientFactory.GetDbCommandClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName);

                await dbCommandClient.DropTableIfExistsAsync(
                    destinationIteration.IterationId,
                    destinationIteration.TempTableName,
                    ct);
                await dbCommandClient.CreateTempTableAsync(
                    destinationIteration.IterationId,
                    destinationIteration.DestinationTable.TableName,
                    destinationIteration.TempTableName,
                    ct);

                destinationIteration =
                    destinationIteration.ChangeState(DestinationTableState.TempTableCreated);

                var rowItemAppend = await RowItemGateway.AppendAsync(destinationIteration, ct);

                //  We want to make sure this is recorded before we start ingesting data into it
                await rowItemAppend.ItemAppendTask;
            }
        }

        private async Task<DestinationTableRowItem> EnsureDestinationIterationAsync(
            SourceTableRowItem sourceTableRowItem,
            TableIdentity destinationTable,
            CancellationToken ct)
        {
            var destinationMap = RowItemGateway.InMemoryCache
                .SourceTableMap[sourceTableRowItem.SourceTable]
                .IterationMap[sourceTableRowItem.IterationId]
                .DestinationMap;

            if (!destinationMap.ContainsKey(destinationTable))
            {
                var tempTableName =
                    $"kc-{destinationTable.TableName}-{Guid.NewGuid().ToString("N")}";
                var destinationIteration = new DestinationTableRowItem
                {
                    State = DestinationTableState.TempTableCreating,
                    SourceTable = sourceTableRowItem.SourceTable,
                    DestinationTable = destinationTable,
                    IterationId = sourceTableRowItem.IterationId,
                    TempTableName = tempTableName
                };
                var rowItemAppend = await RowItemGateway.AppendAsync(destinationIteration, ct);

                //  We want to ensure the item is appended before creating a temp table so
                //  we don't lose track of the table
                await rowItemAppend.ItemAppendTask;

                return destinationIteration;
            }
            else
            {
                var destinationIteration = destinationMap[destinationTable].RowItem;

                return destinationIteration;
            }
        }
    }
}