using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class TempTableCreatingRunner : RunnerBase
    {
        public TempTableCreatingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(10))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                var tempTables = Database.TempTables.Query()
                    .Where(pf => pf.In(
                        t => t.State,
                        [TempTableState.Required, TempTableState.Creating]))
                    .ToImmutableArray();
                var tasks = tempTables
                    .Select(t => Task.Run(() => EnsureTempTableCreatedAsync(t, ct)))
                    .ToImmutableArray();

                await Task.WhenAll(tasks);
                if (!tasks.Any())
                {
                    await SleepAsync(ct);
                }
            }
        }

        private async Task EnsureTempTableCreatedAsync(
            TempTableRecord tempTableRecord,
            CancellationToken ct)
        {
            var activity = Parameterization.Activities[tempTableRecord.IterationKey.ActivityName];
            var destination = activity.GetDestinationTableIdentity();
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                destination.ClusterUri,
                destination.DatabaseName);
            var priority = new KustoPriority(tempTableRecord.IterationKey);

            if (tempTableRecord.State == TempTableState.Creating
                && string.IsNullOrWhiteSpace(tempTableRecord.TempTableName))
            {
                await dbCommandClient.DropTableIfExistsAsync(
                    priority,
                    tempTableRecord.TempTableName,
                    ct);
            }
            tempTableRecord = await PrepareTempTableAsync(tempTableRecord, destination, ct);
            tempTableRecord = await CreateTempTableAsync(
                dbCommandClient,
                priority,
                destination,
                tempTableRecord,
                ct);
        }

        private async Task<TempTableRecord> PrepareTempTableAsync(
            TempTableRecord tempTableRecord,
            TableIdentity destination,
            CancellationToken ct)
        {
            using (var tx = Database.CreateTransaction())
            {
                var tempTableName = $"kc-{destination.TableName}-{Guid.NewGuid().ToString("N")}";
                var newTempTableRecord = tempTableRecord with
                {
                    State = TempTableState.Creating,
                    TempTableName = tempTableName
                };

                Database.TempTables.UpdateRecord(tempTableRecord, newTempTableRecord, tx);

                //  We want to ensure record is persisted (logged) before creating a temp table so
                //  we don't lose track of the table name
                await tx.CompleteAsync(ct);

                return newTempTableRecord;
            }
        }

        private async Task<TempTableRecord> CreateTempTableAsync(
            DbCommandClient dbCommandClient,
            KustoPriority priority,
            TableIdentity destination,
            TempTableRecord tempTableRecord,
            CancellationToken ct)
        {
            await dbCommandClient.CreateTempTableAsync(
                priority,
                destination.TableName,
                tempTableRecord.TempTableName,
                ct);

            var newTempTableRecord = tempTableRecord with
            {
                State = TempTableState.Created
            };

            Database.TempTables.UpdateRecord(tempTableRecord, newTempTableRecord);

            return newTempTableRecord;
        }
    }
}