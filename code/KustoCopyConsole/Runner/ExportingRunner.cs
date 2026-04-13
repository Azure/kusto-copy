using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class ExportingRunner : StartCommandRunnerBase
    {
        public ExportingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(10))
        {
        }

        protected override BlockState InitialState => BlockState.Planned;

        protected override BlockState DestinationState => BlockState.Exporting;

        protected override int? MaxCapacity => Parameterization.ExportCount;

        protected override Uri GetClusterUri(ActivityParameterization activity)
        {
            return activity.GetSourceTableIdentity().ClusterUri;
        }

        protected override async Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var capacity = await dbCommandClient.ShowExportCapacityAsync(
                KustoPriority.HighestPriority,
                ct);

            return capacity;
        }

        protected override async Task<BlockRecord> StartBlockAsync(
            BlockRecord blockRecord,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var sourceTable = activityParam.GetSourceTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                sourceTable.ClusterUri,
                sourceTable.DatabaseName);
            var writableUris = await StagingBlobUriProvider.GetWritableFolderUrisAsync(
                blockRecord.BlockKey,
                ct);
            var iterationRecord = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, blockRecord.BlockKey.IterationKey))
                .First();
            var operationId = await dbClient.ExportBlockAsync(
                new KustoPriority(blockRecord.BlockKey),
                writableUris,
                sourceTable.TableName,
                activityParam.KqlQuery,
                iterationRecord.CursorStart,
                iterationRecord.CursorEnd,
                blockRecord.IngestionTimeStart,
                blockRecord.IngestionTimeEnd,
                ct);
            var newBlockRecord = blockRecord with
            {
                ExportOperationId = operationId
            };

            return newBlockRecord;
        }
    }
}