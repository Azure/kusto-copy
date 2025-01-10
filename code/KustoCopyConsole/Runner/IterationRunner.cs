using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    /// <summary>
    /// Responsible to start and complete iteration.
    /// </summary>
    internal class IterationRunner : RunnerBase
    {
        public IterationRunner(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory)
            : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task<TableRowItem> RunAsync(TableRowItem iterationItem, CancellationToken ct)
        {
            await using (var planningProgress = CreatePlanningProgressBar(iterationItem))
            await using (var exportingProgress =
                CreateBlockStateProgressBar(iterationItem, BlockState.Exporting))
            await using (var queuingProgress =
                CreateBlockStateProgressBar(iterationItem, BlockState.Queued))
            await using (var ingestingProgress =
                CreateBlockStateProgressBar(iterationItem, BlockState.Ingested))
            await using (var movingProgress =
                CreateBlockStateProgressBar(iterationItem, BlockState.ExtentMoved))
            {
                iterationItem = await ProgressRunAsync(iterationItem, ct);

                return iterationItem;
            }
        }

        #region Progress bars
        private ProgressBar CreatePlanningProgressBar(TableRowItem iterationItem)
        {
            return new ProgressBar(
                TimeSpan.FromSeconds(5),
                () =>
                {
                    var iteration = RowItemGateway.InMemoryCache
                    .SourceTableMap[iterationItem.SourceTable]
                    .IterationMap[iterationItem.IterationId];
                    var currentIterationItem = iteration
                    .RowItem;
                    var blockMap = iteration
                    .BlockMap;

                    return new ProgressReport(
                        currentIterationItem.State == TableState.Planning
                        ? ProgessStatus.Progress
                        : ProgessStatus.Completed,
                        $"Planned:  {currentIterationItem.SourceTable.ToStringCompact()}"
                        + $"({currentIterationItem.IterationId}) {blockMap.Count}");
                });
        }

        private ProgressBar CreateBlockStateProgressBar(
            TableRowItem iterationItem,
            BlockState state)
        {
            return new ProgressBar(
                TimeSpan.FromSeconds(10),
                () =>
                {
                    var iteration = RowItemGateway.InMemoryCache
                    .SourceTableMap[iterationItem.SourceTable]
                    .IterationMap[iterationItem.IterationId];
                    var currentIterationItem = iteration
                    .RowItem;

                    if (currentIterationItem.State == TableState.Planning)
                    {
                        return new ProgressReport(ProgessStatus.Nothing, string.Empty);
                    }
                    else
                    {
                        var blockMap = iteration.BlockMap;
                        var stateReachedCount = blockMap.Values
                        .Where(b => b.RowItem.State >= state)
                        .Count();

                        return new ProgressReport(
                            stateReachedCount != blockMap.Count
                            ? ProgessStatus.Progress
                            : ProgessStatus.Completed,
                            $"{state}:  {currentIterationItem.SourceTable.ToStringCompact()}" +
                            $"({currentIterationItem.IterationId}) {stateReachedCount}/{blockMap.Count}");
                    }
                });
        }
        #endregion

        private async Task<TableRowItem> ProgressRunAsync(
            TableRowItem iterationItem,
            CancellationToken ct)
        {
            if (iterationItem.State == TableState.Starting)
            {
                iterationItem = await StartIterationAsync(iterationItem, ct);
            }
            if (iterationItem.State == TableState.Planning)
            {
                iterationItem = await PlanIterationAsync(iterationItem, ct);
            }
            if (iterationItem.State == TableState.Planned)
            {
                iterationItem = await CreateTempTableAsync(iterationItem, ct);
            }
            if (iterationItem.State == TableState.TempTableCreated)
            {
                await ProcessAllBlocksAsync(iterationItem, ct);
            }

            return iterationItem;
        }

        #region Iteration Level
        private async Task<TableRowItem> StartIterationAsync(
            TableRowItem iterationItem,
            CancellationToken ct)
        {
            var queryClient = DbClientFactory.GetDbQueryClient(
                iterationItem.SourceTable.ClusterUri,
                iterationItem.SourceTable.DatabaseName);
            var cursorEnd = await queryClient.GetCurrentCursorAsync(ct);

            iterationItem = iterationItem.ChangeState(TableState.Planning);
            iterationItem.CursorEnd = cursorEnd;
            await RowItemGateway.AppendAsync(iterationItem, ct);

            return iterationItem;
        }

        private async Task<TableRowItem> PlanIterationAsync(
            TableRowItem iterationItem,
            CancellationToken ct)
        {
            var planningRunner =
                new PlanningRunner(Parameterization, RowItemGateway, DbClientFactory);

            iterationItem = await planningRunner.RunAsync(iterationItem, ct);

            return iterationItem;
        }

        private async Task<TableRowItem> CreateTempTableAsync(
            TableRowItem iterationItem,
            CancellationToken ct)
        {
            var tempTableCreatingRunner = new TempTableCreatingRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);

            iterationItem = await tempTableCreatingRunner.RunAsync(iterationItem, ct);

            return iterationItem;
        }
        #endregion

        #region Block Level
        private async Task ProcessAllBlocksAsync(TableRowItem iterationItem, CancellationToken ct)
        {
            var blobPathProvider = GetBlobPathFactory(iterationItem.SourceTable);
            var blockItems = RowItemGateway.InMemoryCache
                .SourceTableMap[iterationItem.SourceTable]
                .IterationMap[iterationItem.IterationId]
                .BlockMap
                .Values
                .Select(b => b.RowItem);
            var processBlockTasks = blockItems
                .Select(b => ProcessSingleBlockAsync(
                    blobPathProvider,
                    iterationItem,
                    b,
                    ct))
                .ToImmutableArray();

            await Task.WhenAll(processBlockTasks);
        }

        private IBlobPathProvider GetBlobPathFactory(TableIdentity sourceTable)
        {
            var activity = Parameterization.Activities
                .Where(a => a.Source.GetTableIdentity() == sourceTable)
                .FirstOrDefault();

            if (activity == null)
            {
                throw new InvalidDataException($"Can't find table in parameters:  {sourceTable}");
            }
            else
            {
                var destinationTable = activity.Destination.GetTableIdentity();
                var tempUriProvider = new TempUriProvider(DbClientFactory.GetDmCommandClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName));

                return tempUriProvider;
            }
        }

        private async Task ProcessSingleBlockAsync(
            IBlobPathProvider blobPathProvider,
            TableRowItem iterationItem,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            blockItem = await ExportBlockAsync(blobPathProvider, iterationItem, blockItem, ct);
            blockItem = await QueueBlockForIngestionAsync(
                blobPathProvider, iterationItem, blockItem, ct);
            blockItem = await AwaitIngestAsync(iterationItem, blockItem, ct);
            blockItem = await MoveExtentsAsync(iterationItem, blockItem, ct);
        }

        private async Task<BlockRowItem> ExportBlockAsync(
            IBlobPathProvider blobPathProvider,
            TableRowItem iterationItem,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var exportingRunner = new ExportingRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);

            blockItem = await exportingRunner.RunAsync(
                blobPathProvider,
                iterationItem,
                blockItem,
                ct);

            return blockItem;
        }

        private async Task<BlockRowItem> QueueBlockForIngestionAsync(
            IBlobPathProvider blobPathProvider,
            TableRowItem iterationItem,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var queueIngestRunner = new QueueIngestRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);

            blockItem = await queueIngestRunner.RunAsync(
                blobPathProvider,
                blockItem,
                ct);

            return blockItem;
        }

        private async Task<BlockRowItem> AwaitIngestAsync(
            TableRowItem iterationItem,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var awaitIngestRunner = new AwaitIngestRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);

            blockItem = await awaitIngestRunner.RunAsync(blockItem, ct);

            return blockItem;
        }

        private async Task<BlockRowItem> MoveExtentsAsync(
            TableRowItem iterationItem,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var moveExtentsRunner = new MoveExtentsRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);

            blockItem = await moveExtentsRunner.RunAsync(blockItem, ct);

            return blockItem;
        }
        #endregion
    }
}
