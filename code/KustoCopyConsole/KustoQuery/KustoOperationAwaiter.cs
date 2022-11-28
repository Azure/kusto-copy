using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Concurrency;
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
        }
        #endregion

        private const string IN_PROGRESS_STATE = "InProgress";
        private const string FAILED_STATE = "Failed";
        private const string THROTTLED_STATE = "Throttled";

        private static readonly TimeSpan WAIT_BETWEEN_CHECKS = TimeSpan.FromSeconds(1);

        private readonly KustoQueuedClient _kustoClient;
        private readonly SingletonExecution _singletonExecution = new SingletonExecution();
        private readonly ConcurrentDictionary<Guid, OperationState> _operations =
            new ConcurrentDictionary<Guid, OperationState>();

        public KustoOperationAwaiter(KustoQueuedClient kustoClient)
        {
            _kustoClient = kustoClient;
        }

        public async Task RunAsynchronousOperationAsync(Guid operationId)
        {
            var thisOperationState = new OperationState();

            await WaitForOperationToCompleteAsync(operationId, thisOperationState);
            if (!_operations.Remove(operationId, out _))
            {
                throw new InvalidOperationException("Operation ID wasn't present");
            }
            else if (thisOperationState.State == FAILED_STATE)
            {
                throw new CopyException(
                    $"Operation {operationId} failed with message:  "
                    + $"'{thisOperationState.Status}'");
            }
            else if (thisOperationState.State == THROTTLED_STATE)
            {
                throw new CopyException(
                    $"Operation {operationId} has been throttled with message:  "
                    + $"'{thisOperationState.Status}'");
            }
        }

        public async Task<IImmutableList<T>> RunAsynchronousOperationAsync<T>(
            Guid operationId,
            Func<IDataRecord, T> projection)
        {
            await RunAsynchronousOperationAsync(operationId);

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
                await _singletonExecution.SingleRunAsync(async () =>
                {
                    await Task.Delay(WAIT_BETWEEN_CHECKS);

                    var operationIdList = string.Join(
                        ", ",
                        _operations.Keys.Select(id => $"'{id}'"));
                    var commandText = @$"
.show operations ({operationIdList})
| project OperationId, State, Status
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
                            Status = (string)r["Status"]
                        });

                    foreach (var info in operationInfo)
                    {
                        var operationState = _operations[info.OperationId];

                        operationState.State = info.State;
                        operationState.Status = info.Status;
                    }
                });
            }
            while (thisOperationState.State == IN_PROGRESS_STATE);
        }
    }
}