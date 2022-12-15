using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Exceptions;
using KustoCopyConsole.Concurrency;
using Polly;
using Polly.Bulkhead;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public class KustoOperationAwaiter
    {
        #region Inner Types
        private class OperationState
        {
            public string State { get; set; } = IN_PROGRESS_STATE;

            public string Status { get; set; } = string.Empty;

            public string Database { get; set; } = string.Empty;

            public bool ShouldRetry { get; set; } = false;
        }
        #endregion

        private const string IN_PROGRESS_STATE = "InProgress";
        private const string COMPLETED_STATE = "Completed";

        private static readonly TimeSpan WAIT_BETWEEN_CHECKS = TimeSpan.FromSeconds(1);

        private readonly KustoQueuedClient _kustoClient;
        private readonly AsyncBulkheadPolicy _bulkheadPolicy =
            Policy.BulkheadAsync(1, int.MaxValue);
        private readonly ConcurrentDictionary<Guid, OperationState> _operations =
            new ConcurrentDictionary<Guid, OperationState>();

        public KustoOperationAwaiter(KustoQueuedClient kustoClient)
        {
            _kustoClient = kustoClient;
        }

        public async Task RunAsynchronousOperationAsync(
            Guid operationId,
            string operationType)
        {
            var thisOperationState = new OperationState();

            await WaitForOperationToCompleteAsync(operationId, thisOperationState);
            if (!_operations.Remove(operationId, out _))
            {
                throw new InvalidOperationException("Operation ID wasn't present");
            }
            else if (thisOperationState.State != COMPLETED_STATE)
            {
                throw new KustoServiceException(
                    "0000",
                    "AsyncOperationNotCompleting",
                    $"Operation {operationId} ({operationType}) failed with status "
                    + $"'{thisOperationState.Status}'",
                    _kustoClient.HostName,
                    thisOperationState.Database,
                    string.Empty,
                    Guid.Empty,
                    isPermanent: !thisOperationState.ShouldRetry);
            }
        }

        public async Task<IImmutableList<T>> RunAsynchronousOperationAsync<T>(
            Guid operationId,
            string operationType,
            Func<IDataRecord, T> projection)
        {
            await RunAsynchronousOperationAsync(operationId, operationType);

            return await FetchOperationDetailsAsync(operationId, projection);
        }

        private async Task<IImmutableList<T>> FetchOperationDetailsAsync<T>(
            Guid operationId,
            Func<IDataRecord, T> projection)
        {
            var commandText = $".show operation {operationId} details";
            var operationDetails = await _kustoClient
                .ExecuteCommandAsync(
                KustoPriority.HighestPriority,
                string.Empty,
                commandText,
                r => projection(r));

            return operationDetails;
        }

        private async Task WaitForOperationToCompleteAsync(
            Guid operationId,
            OperationState thisOperationState)
        {
            _operations.TryAdd(operationId, thisOperationState);
            do
            {
                await _bulkheadPolicy.ExecuteAsync(async () =>
                {
                    await Task.Delay(WAIT_BETWEEN_CHECKS);

                    var operationIdList = string.Join(
                        ", ",
                        _operations.Keys.Select(id => $"'{id}'"));
                    var commandText = @$"
.show operations ({operationIdList})
| project OperationId, State, Status, Database, ShouldRetry
| where State != '{IN_PROGRESS_STATE}'";
                    var operationInfo = await _kustoClient
                        .ExecuteCommandAsync(
                        KustoPriority.HighestPriority,
                        string.Empty,
                        commandText,
                        r => new
                        {
                            OperationId = (Guid)r["OperationId"],
                            State = (string)r["State"],
                            Status = (string)r["Status"],
                            Database = (string)r["Database"],
                            ShouldRetry = (SByte)r["ShouldRetry"]
                        });

                    foreach (var info in operationInfo)
                    {
                        //  It is possible the operation ID get removes as a racing condition
                        //  by the owning thread
                        if (_operations.TryGetValue(info.OperationId, out var operationState))
                        {
                            operationState.State = info.State;
                            operationState.Status = info.Status;
                            operationState.Database = info.Database;
                            operationState.ShouldRetry = info.ShouldRetry == 1;
                        }
                    }
                });
            }
            while (thisOperationState.State == IN_PROGRESS_STATE);
        }
    }
}