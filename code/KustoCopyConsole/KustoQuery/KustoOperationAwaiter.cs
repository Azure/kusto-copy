using KustoCopyConsole.Concurrency;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        public async Task WaitForOperationCompletionAsync(string databaseName, Guid operationId)
        {
            var thisOperationState = new OperationState();

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
.show operations
({operationIdList})
| project OperationId, State, Status
| where State != '{IN_PROGRESS_STATE}'";
                    var operationDetails = await _kustoClient
                        .ExecuteCommandAsync(
                        KustoPriority.HighestPriority,
                        databaseName,
                        commandText,
                        r => new
                        {
                            OperationId = (Guid)r["OperationId"],
                            State = (string)r["State"],
                            Status = (string)r["Status"]
                        });

                    foreach (var detail in operationDetails)
                    {
                        var operationState = _operations[detail.OperationId];

                        operationState.State = detail.State;
                        operationState.Status = detail.Status;
                    }
                });
            }
            while (thisOperationState.State == IN_PROGRESS_STATE);

            if (!_operations.Remove(operationId, out _))
            {
                throw new InvalidOperationException("Operation ID wasn't present");
            }

            if (thisOperationState.State == FAILED_STATE)
            {
                throw new CopyException(
                    $"Operation {operationId} failed with message:  "
                    + $"'{thisOperationState.Status}'");
            }
            if (thisOperationState.State == THROTTLED_STATE)
            {
                throw new CopyException(
                    $"Operation {operationId} has been throttled with message:  "
                    + $"'{thisOperationState.Status}'");
            }
        }
    }
}