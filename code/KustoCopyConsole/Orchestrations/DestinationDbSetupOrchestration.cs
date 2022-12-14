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
| serialize
| extend RowId = row_number()
| summarize arg_max(RowId, *) by
    {nameof(StatusItem.IterationId)},
    {nameof(StatusItem.SubIterationId)},
    {nameof(StatusItem.TableName)},
    {nameof(StatusItem.RecordBatchId)}
| where {nameof(StatusItem.State)} != ""{StatusItemState.Deleted}""
| extend Level=case(
    isnotnull({nameof(StatusItem.SubIterationId)}) and isnotnull({nameof(StatusItem.RecordBatchId)}),
    ""RecordBatch"",
    isnotnull({nameof(StatusItem.SubIterationId)}),
    ""SubIteration"",
    ""Iteration"")
| project Level, {columnListText}
| order by
    {nameof(StatusItem.IterationId)} desc,
    iif(isnotnull({nameof(StatusItem.SubIterationId)}), {nameof(StatusItem.SubIterationId)}, {long.MaxValue}) desc,
    iif(isnotnull({nameof(StatusItem.RecordBatchId)}), {nameof(StatusItem.RecordBatchId)}, 0) asc
}}";

            await destinationClient.ExecuteCommandAsync(
                KustoPriority.HighestPriority,
                dbStatus.DbName,
                createStatusViewFunction,
                r => r);
        }
    }
}