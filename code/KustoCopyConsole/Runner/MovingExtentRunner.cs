using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class MovingExtentRunner : StartCommandRunnerBase
    {
        private const int MAXIMUM_EXTENT_MOVING = 500;

        public MovingExtentRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(15))
        {
        }

        protected override BlockState InitialState => BlockState.Ingested;

        protected override BlockState DestinationState => BlockState.ExtentMoving;

        protected override int? MaxCapacity => null;

        protected override Uri GetClusterUri(ActivityParameterization activity)
        {
            return activity.GetDestinationTableIdentity().ClusterUri;
        }

        protected override async Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var capacity = await dbCommandClient.ShowClusterNodeCountAsync(
                KustoPriority.HighestPriority,
                ct);

            return capacity;
        }

        protected override async Task<BlockRecord> StartBlockAsync(
            BlockRecord blockRecord,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var destinationTable = activityParam.GetDestinationTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                destinationTable.ClusterUri,
                destinationTable.DatabaseName);
            var tempTableName = GetTempTable(blockRecord.BlockKey.IterationKey).TempTableName;
            var extentIds = Database.Extents.Query()
                .Where(pf => pf.Equal(e => e.BlockKey, blockRecord.BlockKey))
                .Select(e => e.ExtentId)
                .ToArray();
            var operationId = await dbClient.MoveExtentsAsync(
                new KustoPriority(blockRecord.BlockKey),
                tempTableName,
                destinationTable.TableName,
                extentIds,
                ct);
            var newBlockRecord = blockRecord with
            {
                MoveOperationId = operationId
            };

            return newBlockRecord;
        }
    }
}