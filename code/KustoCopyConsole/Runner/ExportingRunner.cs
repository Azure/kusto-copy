﻿using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class ExportingRunner : RunnerBase
    {
        public ExportingRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task<BlockRowItem> RunAsync(
            IStagingBlobUriProvider blobPathProvider,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var activityItem = RowItemGateway.InMemoryCache
                .ActivityMap[blockItem.ActivityName]
                .RowItem;
            var activityParam = Parameterization.Activities[blockItem.ActivityName];
            var exportClient = DbClientFactory.GetExportClient(
                activityItem.SourceTable.ClusterUri,
                activityItem.SourceTable.DatabaseName,
                activityItem.SourceTable.TableName,
                activityParam.KqlQuery);

            if (blockItem.State == BlockState.CompletingExport)
            {
                blockItem = CleanUrls(blockItem);
            }
            if (blockItem.State == BlockState.Planned)
            {
                blockItem = await ExportBlockAsync(blobPathProvider, exportClient, blockItem, ct);
            }
            if (blockItem.State == BlockState.Exporting)
            {
                blockItem = await AwaitExportBlockAsync(exportClient, blockItem, ct);
            }

            return blockItem;
        }

        private async Task<BlockRowItem> AwaitExportBlockAsync(
            ExportClient exportClient,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var exportDetails = await exportClient.AwaitExportAsync(
                new KustoPriority(
                    blockItem.ActivityName, blockItem.IterationId, blockItem.BlockId),
                blockItem.OperationId,
                ct);
            var urlItems = exportDetails
                .Select(e => new UrlRowItem
                {
                    State = UrlState.Exported,
                    ActivityName = blockItem.ActivityName,
                    IterationId = blockItem.IterationId,
                    BlockId = blockItem.BlockId,
                    Url = e.BlobUri.ToString(),
                    RowCount = e.RecordCount
                });

            if (!urlItems.Any())
            {
                throw new InvalidDataException("No URL exported");
            }
            blockItem = blockItem.ChangeState(BlockState.CompletingExport);
            RowItemGateway.Append(blockItem);
            foreach (var urlItem in urlItems)
            {
                RowItemGateway.Append(urlItem);
            }
            blockItem = blockItem.ChangeState(BlockState.Exported);
            RowItemGateway.Append(blockItem);

            return blockItem;
        }

        private async Task<BlockRowItem> ExportBlockAsync(
            IStagingBlobUriProvider blobPathProvider,
            ExportClient exportClient,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var iteration = RowItemGateway.InMemoryCache
                .ActivityMap[blockItem.ActivityName]
                .IterationMap[blockItem.IterationId]
                .RowItem;
            var operationId = await exportClient.NewExportAsync(
                new KustoPriority(
                    blockItem.ActivityName,
                    blockItem.IterationId,
                    blockItem.BlockId),
                blobPathProvider,
                blockItem.IterationId,
                blockItem.BlockId,
                iteration.CursorStart,
                iteration.CursorEnd,
                blockItem.IngestionTimeStart,
                blockItem.IngestionTimeEnd,
                ct);

            blockItem = blockItem.ChangeState(BlockState.Exporting);
            blockItem.OperationId = operationId;
            RowItemGateway.Append(blockItem);

            return blockItem;
        }

        private BlockRowItem CleanUrls(BlockRowItem blockItem)
        {
            var existingUrls = RowItemGateway.InMemoryCache
                .ActivityMap[blockItem.ActivityName]
                .IterationMap[blockItem.IterationId]
                .BlockMap[blockItem.BlockId]
                .UrlMap
                .Values;

            foreach (var url in existingUrls)
            {
                RowItemGateway.Append(url.RowItem.ChangeState(UrlState.Deleted));
            }
            blockItem = blockItem.ChangeState(BlockState.Exporting);
            RowItemGateway.Append(blockItem);

            return blockItem;
        }
    }
}