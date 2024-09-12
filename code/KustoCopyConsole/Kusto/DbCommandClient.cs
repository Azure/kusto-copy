﻿using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System.Collections.Immutable;
using System.Data;

namespace KustoCopyConsole.Kusto
{
    internal class DbCommandClient
    {
        private readonly Random _random = new();
        private readonly ICslAdminProvider _provider;
        private readonly PriorityExecutionQueue<KustoDbPriority> _commandQueue;
        private readonly string _databaseName;

        public DbCommandClient(
            ICslAdminProvider provider,
            PriorityExecutionQueue<KustoDbPriority> commandQueue,
            string databaseName)
        {
            _provider = provider;
            _commandQueue = commandQueue;
            _databaseName = databaseName;
        }

        public async Task<int> GetExportCapacityAsync()
        {
            return await _commandQueue.RequestRunAsync(
                KustoDbPriority.HighestPriority,
                async () =>
                {
                    var commandText = @"
.show capacity
| where Resource == 'DataExport'
| project Total";
                    var reader = await _provider.ExecuteControlCommandAsync(
                        _databaseName,
                        commandText);
                    var exportCapacity = (long)reader.ToDataSet().Tables[0].Rows[0][0];

                    return (int)exportCapacity;
                });
        }

        public async Task ShowOperationsAsync(IEnumerable<string> operationIds, CancellationToken ct)
        {
            if (operationIds.Any())
            {
                await _commandQueue.RequestRunAsync(
                    KustoDbPriority.HighestPriority,
                    async () =>
                    {
                        var operationIdsText = string.Join(", ", operationIds);
                        var commandText = @$".show operations({operationIdsText})";
                        var reader = await _provider.ExecuteControlCommandAsync(
                            _databaseName,
                            commandText);
                        var result = reader.ToDataSet().Tables[0].Rows
                            .Cast<DataRow>()
                            .Select(r => new
                            {
                                OperationId = ((Guid)r["OperationId"]).ToString(),
                                State = (string)r["State"],
                                Status = (string)r["Status"],
                                ShouldRetry = (bool)r["ShouldRetry"]
                            })
                            .ToImmutableArray();
                    });
            }
        }

        public async Task<string> ExportBlockAsync(
            IImmutableList<Uri> storageRoots,
            string tableName,
            string cursorStart,
            string cursorEnd,
            string ingestionTimeStart,
            string ingestionTimeEnd,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                KustoDbPriority.HighestPriority,
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";
                    const string INGESTION_TIME_START_PARAM = "ingestionTimeStartText";
                    const string INGESTION_TIME_END_PARAM = "ingestionTimeEndText";

                    //  Shuffle the storage roots for better long term round robin
                    var shuffledStorageRoots = storageRoots
                        .OrderBy(i => _random.Next());
                    var quotedRoots = shuffledStorageRoots
                        .Select(r => @$"h""{r}""");
                    var rootsText = string.Join(", ", quotedRoots);
                    var commandText = @$"
.export async compressed to parquet (
    {rootsText}
)
with (
    namePrefix=""export"",
    persistDetails=true,
    parquetDatetimePrecision=""microsecond""
) <| 
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string,
    {INGESTION_TIME_START_PARAM}:string,
    {INGESTION_TIME_END_PARAM}:string);
let IngestionTimeStart = todatetime({INGESTION_TIME_START_PARAM});
let BlockData = ['{tableName}']
    | project IngestionTime = ingestion_time()
    | where iif(isempty({CURSOR_START_PARAM}), true, cursor_after({CURSOR_START_PARAM}))
    | where iif(isempty({CURSOR_END_PARAM}), true, cursor_before_or_at({CURSOR_END_PARAM}))
    | where iif(isempty({INGESTION_TIME_START_PARAM}), true, IngestionTime>IngestionTimeStart);
    | where iif(isempty({INGESTION_TIME_END_PARAM}), true, IngestionTime>IngestionTimeEnd);
BlockData
";
                    var properties = new ClientRequestProperties();

                    properties.SetParameter(CURSOR_START_PARAM, cursorStart);
                    properties.SetParameter(CURSOR_END_PARAM, cursorEnd);
                    properties.SetParameter(INGESTION_TIME_START_PARAM, ingestionTimeStart);
                    properties.SetParameter(INGESTION_TIME_END_PARAM, ingestionTimeEnd);

                    var reader = await _provider.ExecuteControlCommandAsync(
                        string.Empty,
                        commandText,
                        properties);
                    var operationId = (Guid)reader.ToDataSet().Tables[0].Rows[0][0];

                    return operationId.ToString();
                });
        }
    }
}