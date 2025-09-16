using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class TempTableCreatingRunner : RunnerBase
    {
        public TempTableCreatingRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(10))
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
                    .Select(t => EnsureTempTableCreatedAsync(t, ct))
                    .ToImmutableArray();

                await Task.WhenAll(tasks);
                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task EnsureTempTableCreatedAsync(
            TempTableRecord tempTableRecord,
            CancellationToken ct)
        {
            var activity = Parameterization.Activities[tempTableRecord.IterationKey.ActivityName];
            var destination = activity.Destination.GetTableIdentity();
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
            using (var tx = Database.Database.CreateTransaction())
            {
                var tempTableName = $"kc-{destination.TableName}-{Guid.NewGuid().ToString("N")}";

                tempTableRecord = tempTableRecord with
                {
                    State = TempTableState.Creating,
                    TempTableName = tempTableName
                };
                Database.TempTables.Query(tx)
                    .Where(pf => pf.MatchKeys(
                        tempTableRecord,
                        t => t.IterationKey.ActivityName,
                        t => t.IterationKey.IterationId))
                    .Delete();
                Database.TempTables.AppendRecord(tempTableRecord, tx);

                //  We want to ensure record is persisted (logged) before creating a temp table so
                //  we don't lose track of the table name
                await tx.CompleteAndLogAsync();

                return tempTableRecord;
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
            tempTableRecord = tempTableRecord with
            {
                State = TempTableState.Created
            };
            using (var tx = Database.Database.CreateTransaction())
            {
                Database.TempTables.Query(tx)
                    .Where(pf => pf.MatchKeys(
                        tempTableRecord,
                        t => t.IterationKey.ActivityName,
                        t => t.IterationKey.IterationId))
                    .Delete();
                Database.TempTables.AppendRecord(tempTableRecord);
                
                tx.Complete();
            }

            return tempTableRecord;
        }
    }
}