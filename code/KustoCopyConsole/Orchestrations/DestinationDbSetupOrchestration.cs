using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    internal class DestinationDbSetupOrchestration
    {
        private const string STATUS_VIEW_NAME = "KC_Status";

        internal static async Task SetupAsync(
            DatabaseStatus dbStatus,
            KustoQueuedClient destinationClient,
            CancellationToken ct)
        {
            var columnListText = string.Join(
                ", ",
                StatusItem.ExternalTableSchema
                .Split(',')
                .Select(c => c.Split(':').First()));
            var createStatusViewFunction = $@".create-or-alter function with
(docstring = 'Latest state view on checkpoint blob', folder='Kusto Copy')
{STATUS_VIEW_NAME}(){{
externaldata({StatusItem.ExternalTableSchema})
[
   '{dbStatus.IndexBlobUri};impersonate'
]
with(format='csv', ignoreFirstRecord=true)
| summarize arg_max({nameof(StatusItem.Timestamp)}, *) by
    {nameof(StatusItem.IterationId)},
    {nameof(StatusItem.SubIterationId)},
    {nameof(StatusItem.TableName)},
    {nameof(StatusItem.RecordBatchId)}
| order by {nameof(StatusItem.IterationId)} asc
| project {columnListText}
}}";

            await destinationClient.ExecuteCommandAsync(
                new KustoPriority(),
                dbStatus.DbName,
                createStatusViewFunction,
                r => r);
        }
    }
}