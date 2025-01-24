using Azure.Core;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.RowItems.Keys;
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
            TokenCredential credential,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(10))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var taskMap = new Dictionary<IterationKey, Task>();

            while (taskMap.Any() || !AllActivitiesCompleted())
            {
                var newIterations = RowItemGateway.InMemoryCache.ActivityMap
                     .Values
                     .SelectMany(a => a.IterationMap.Values)
                     .Where(i => i.RowItem.State >= IterationState.Planning)
                     .Where(i => i.TempTable == null
                     || i.TempTable.State == TempTableState.Creating)
                     .Select(i => new
                     {
                         Key = i.RowItem.GetIterationKey(),
                         Iteration = i
                     })
                     .Where(o => !taskMap.ContainsKey(o.Key));

                foreach (var o in newIterations)
                {
                    taskMap.Add(o.Key, EnsureTempTableCreatedAsync(o.Iteration, ct));
                }
                await CleanTaskMapAsync(taskMap);
                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task CleanTaskMapAsync(IDictionary<IterationKey, Task> taskMap)
        {
            foreach (var taskKey in taskMap.Keys.ToImmutableArray())
            {
                var task = taskMap[taskKey];

                if (task.IsCompleted)
                {
                    await task;
                    taskMap.Remove(taskKey);
                }
            }
        }

        private async Task EnsureTempTableCreatedAsync(
            IterationCache iteration,
            CancellationToken ct)
        {
            var doesPreExist = iteration.TempTable != null;

            if (!doesPreExist)
            {
                await PrepareTempTableAsync(iteration.RowItem, ct);
                iteration = RowItemGateway.InMemoryCache
                    .ActivityMap[iteration.RowItem.ActivityName]
                    .IterationMap[iteration.RowItem.IterationId];
            }
            if (iteration.TempTable!.State == TempTableState.Creating)
            {
                await CreateTempTableAsync(iteration.TempTable, doesPreExist, ct);
            }
        }

        private async Task PrepareTempTableAsync(
            IterationRowItem iterationItem,
            CancellationToken ct)
        {
            var activity = RowItemGateway.InMemoryCache
                .ActivityMap[iterationItem.ActivityName]
                .RowItem;
            var tempTableName =
                $"kc-{activity.DestinationTable.TableName}-{Guid.NewGuid().ToString("N")}";
            var tempTableItem = new TempTableRowItem
            {
                State = TempTableState.Creating,
                ActivityName = iterationItem.ActivityName,
                IterationId = iterationItem.IterationId,
                TempTableName = tempTableName
            };

            //  We want to ensure the item is appended before creating a temp table so
            //  we don't lose track of the table
            await RowItemGateway.AppendAndPersistAsync(tempTableItem, ct);
        }

        private async Task CreateTempTableAsync(
            TempTableRowItem tempTableItem,
            bool doesPreExist,
            CancellationToken ct)
        {
            var activity = RowItemGateway.InMemoryCache
                .ActivityMap[tempTableItem.ActivityName]
                .RowItem;
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                activity.DestinationTable.ClusterUri,
                activity.DestinationTable.DatabaseName);
            var priority = new KustoPriority(tempTableItem.GetIterationKey());

            if (doesPreExist)
            {
                await dbCommandClient.DropTableIfExistsAsync(
                    priority,
                    tempTableItem.TempTableName,
                    ct);
            }
            await dbCommandClient.CreateTempTableAsync(
                priority,
                activity.DestinationTable.TableName,
                tempTableItem.TempTableName,
                ct);

            tempTableItem = tempTableItem.ChangeState(TempTableState.Created);
            RowItemGateway.Append(tempTableItem);
        }
    }
}