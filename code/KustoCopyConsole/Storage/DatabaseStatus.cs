using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using CsvHelper;
using KustoCopyConsole.Orchestrations;
using Microsoft.Identity.Client.Platforms.Features.DesktopOs.Kerberos;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    public class DatabaseStatus
    {
        #region Inner Types
        private class StatusItemIndex
        {
            #region Inner Types
            private class RecordBatchIndex
            {
                public ConcurrentDictionary<long, StatusItem> Index { get; } =
                    new ConcurrentDictionary<long, StatusItem>();
            }

            private class HierarchicalIndex<INDEX>
            {
                public HierarchicalIndex(StatusItem parent, INDEX children)
                {
                    Parent = parent;
                    Children = children;
                }

                public StatusItem Parent { get; set; }

                public INDEX Children { get; }
            }

            private class SubIterationIndex
            {
                public ConcurrentDictionary<long, HierarchicalIndex<RecordBatchIndex>> Index { get; } =
                    new ConcurrentDictionary<long, HierarchicalIndex<RecordBatchIndex>>();
            }

            private class IterationIndex
            {
                public ConcurrentDictionary<long, HierarchicalIndex<SubIterationIndex>> Index { get; } =
                    new ConcurrentDictionary<long, HierarchicalIndex<SubIterationIndex>>();
            }
            #endregion

            private readonly IterationIndex _iterationIndex = new IterationIndex();

            public IEnumerable<StatusItem> AllItems
            {
                get
                {
                    foreach (var iteration in _iterationIndex.Index.Values)
                    {
                        yield return iteration.Parent;
                        foreach (var subIteration in iteration.Children.Index.Values)
                        {
                            yield return subIteration.Parent;
                            foreach (var recordBatch in subIteration.Children.Index.Values)
                            {
                                yield return recordBatch;
                            }
                        }
                    }
                }
            }

            public IImmutableList<StatusItem> Iterations
            {
                get
                {
                    var iterations = _iterationIndex.Index.Values
                        .Select(i => i.Parent)
                        .ToImmutableArray();

                    return iterations;
                }
            }

            public StatusItem GetIteration(long iterationId)
            {
                if (_iterationIndex.Index.TryGetValue(iterationId, out var subIterationIndex))
                {
                    return subIterationIndex.Parent;
                }
                else
                {
                    throw new ArgumentOutOfRangeException($"Can't find iteration '{iterationId}'");
                }
            }

            public IImmutableList<StatusItem> GetSubIterations(long iterationId)
            {
                var items = _iterationIndex
                    .Index[iterationId]
                    .Children
                    .Index
                    .Values
                    .Select(v => v.Parent)
                    .ToImmutableArray();

                return items;
            }

            public StatusItem GetSubIteration(long iterationId, long subIterationId)
            {
                return _iterationIndex
                    .Index[iterationId]
                    .Children
                    .Index[subIterationId]
                    .Parent;
            }

            public IImmutableList<StatusItem> GetRecordBatches(
                long iterationId,
                long subIterationId)
            {
                var items = _iterationIndex
                    .Index[iterationId]
                    .Children
                    .Index[subIterationId]
                    .Children
                    .Index
                    .Values
                    .ToImmutableArray();

                return items;
            }

            public StatusItem GetRecordBatch(
                long iterationId,
                long subIterationId,
                long recordBatchId)
            {
                var item = _iterationIndex
                    .Index[iterationId]
                    .Children
                    .Index[subIterationId]
                    .Children
                    .Index[recordBatchId];

                return item;
            }

            public void IndexNewItem(StatusItem item)
            {
                switch (item.Level)
                {
                    case HierarchyLevel.Iteration:
                        IndexNewIteration(item);
                        return;
                    case HierarchyLevel.SubIteration:
                        IndexNewSubIteration(item);
                        return;
                    case HierarchyLevel.RecordBatch:
                        IndexNewRecordBatch(item);
                        return;

                    default:
                        throw new NotSupportedException($"Level not supported:  '{item.Level}'");
                }
            }

            private void IndexNewIteration(StatusItem item)
            {
                if (_iterationIndex.Index.TryGetValue(item.IterationId, out var iteration))
                {
                    iteration.Parent = item;
                }
                else
                {
                    if (!_iterationIndex.Index.TryAdd(
                        item.IterationId,
                        new HierarchicalIndex<SubIterationIndex>(item, new SubIterationIndex())))
                    {
                        throw new InvalidOperationException(
                            "Internal Index corruption at iteration level");
                    }
                }
            }

            private void IndexNewSubIteration(StatusItem item)
            {
                if (_iterationIndex.Index.TryGetValue(item.IterationId, out var iteration))
                {
                    if (iteration.Children.Index.TryGetValue(
                        item.SubIterationId!.Value,
                        out var subIteration))
                    {
                        subIteration.Parent = item;
                    }
                    else
                    {
                        if (!iteration.Children.Index.TryAdd(
                            item.SubIterationId!.Value,
                            new HierarchicalIndex<RecordBatchIndex>(
                                item,
                                new RecordBatchIndex())))
                        {
                            throw new InvalidOperationException(
                                "Internal Index corruption at sub-iteration level");
                        }
                    }
                }
                else
                {
                    throw new NotSupportedException(
                        "Can't insert a sub iteration where iteration doesn't exist");
                }
            }

            private void IndexNewRecordBatch(StatusItem item)
            {
                if (_iterationIndex.Index.TryGetValue(item.IterationId, out var iteration))
                {
                    if (iteration.Children.Index.TryGetValue(
                        item.SubIterationId!.Value,
                        out var subIteration))
                    {
                        subIteration.Children.Index.AddOrUpdate(
                            item.RecordBatchId!.Value,
                            (_) => item,
                            (_, _) => item);
                    }
                    else
                    {
                        throw new NotSupportedException(
                            "Can't insert a record batch where sub iteration doesn't exist");
                    }
                }
                else
                {
                    throw new NotSupportedException(
                        "Can't insert a sub iteration where iteration doesn't exist");
                }
            }
        }
        #endregion

        private readonly StatusItemIndex _statusIndex = new StatusItemIndex();
        private CheckpointGateway _checkpointGateway;

        #region Constructors
        public static async Task<DatabaseStatus> RetrieveAsync(
            string dbName,
            DataLakeDirectoryClient lakeFolderClient,
            BlobContainerClient lakeContainerClient,
            CancellationToken ct)
        {
            var checkpointGateway = new CheckpointGateway(
                lakeFolderClient.GetSubDirectoryClient($"db.{dbName}"),
                lakeContainerClient);

            if (!(await checkpointGateway.ExistsAsync(ct)))
            {
                await checkpointGateway.CreateAsync(ct);

                await PersistItemsAsync(checkpointGateway, new StatusItem[0], true, false, ct);

                return new DatabaseStatus(
                    dbName,
                    checkpointGateway,
                    new StatusItem[0]);
            }
            else
            {
                var buffer = await checkpointGateway.ReadAllContentAsync(ct);

                //  Rewrite the content in one clean append-blob
                //  Ensure it's an append blob + compact it
                Trace.TraceInformation("Rewrite checkpoint blob...");

                var items = ParseCsv(buffer);
                var databaseStatus = new DatabaseStatus(dbName, checkpointGateway, items);

                await databaseStatus.CompactAsync(ct);

                return databaseStatus;
            }
        }

        private DatabaseStatus(
            string dbName,
            CheckpointGateway checkpointGateway,
            IEnumerable<StatusItem> statusItems)
        {
            var latestItems = statusItems
                .Zip(Enumerable.Range(0, statusItems.Count()), (item, i) => new
                {
                    Item = item,
                    RowId = i
                })
                .GroupBy(i => (i.Item.IterationId, i.Item.SubIterationId, i.Item.TableName, i.Item.RecordBatchId))
                .Select(g => g.MaxBy(i => i.RowId)!.Item);

            DbName = dbName;
            _checkpointGateway = checkpointGateway;
            foreach (var item in latestItems)
            {
                ProcessNewItem(item);
            }
        }
        #endregion

        /// <summary>Add / update of status.</summary>
        public event EventHandler? StatusChanged;

        public string DbName { get; }

        public Uri IndexBlobUri => _checkpointGateway.BlobUri;
        
        public DataLakeDirectoryClient IndexFolderClient => _checkpointGateway.FolderClient;

        public IImmutableList<StatusItem> GetIterations()
        {
            return _statusIndex.Iterations;
        }

        public StatusItem GetIteration(long iterationId)
        {
            return _statusIndex.GetIteration(iterationId);
        }

        public IImmutableList<StatusItem> GetSubIterations(long iterationId)
        {
            return _statusIndex.GetSubIterations(iterationId);
        }

        public StatusItem GetSubIteration(long iterationId, long subIterationId)
        {
            return _statusIndex.GetSubIteration(iterationId, subIterationId);
        }

        public IImmutableList<StatusItem> GetRecordBatches(long iterationId, long subIterationId)
        {
            return _statusIndex.GetRecordBatches(iterationId, subIterationId);
        }

        public StatusItem GetRecordBatch(long iterationId, long subIterationId, long recordBatchId)
        {
            return _statusIndex.GetRecordBatch(iterationId, subIterationId, recordBatchId);
        }

        public CursorWindow GetCursorWindow(long iterationId)
        {
            var iteration = GetIteration(iterationId);

            if (iterationId == 1)
            {
                return new CursorWindow(null, iteration.EndCursor);
            }
            else
            {
                var previousIteration = GetIteration(iterationId - 1);

                return new CursorWindow(previousIteration.EndCursor, previousIteration.EndCursor);
            }
        }

        public async Task RollupStatesAsync(
            StatusItemState from,
            StatusItemState to,
            CancellationToken ct)
        {
            var itemsToUpdate = new List<StatusItem>();
            var subIterations = GetIterations()
                .Where(i => i.State <= from)
                .SelectMany(i => GetSubIterations(i.IterationId))
                .Where(s => s.State == from);

            //  Detect sub iterations where all records are in "from"
            foreach (var subIteration in subIterations)
            {
                var allRecords = GetRecordBatches(
                    subIteration.IterationId,
                    subIteration.SubIterationId!.Value);

                if (!allRecords.Any(r => r.State < to))
                {
                    itemsToUpdate.Add(subIteration.UpdateState(to));
                }
            }

            var iterations = GetIterations()
                .Where(i => i.State == from);

            //  Detect iterations where all sub iterations have been moved
            foreach (var iteration in iterations)
            {
                var nonReadySubIterations = GetSubIterations(iteration.IterationId)
                    .Where(s => s.State <= from)
                    //  Check we didn't slate them for being exported already
                    .Where(s => !itemsToUpdate.Any(
                        i => i.IterationId == s.IterationId
                        && i.SubIterationId == s.SubIterationId));

                if (!nonReadySubIterations.Any())
                {
                    itemsToUpdate.Add(iteration.UpdateState(to));
                }
            }

            if (itemsToUpdate.Count > 0)
            {
                await PersistNewItemsAsync(itemsToUpdate, ct);
            }
        }

        public async Task PersistNewItemsAsync(
            IEnumerable<StatusItem> items,
            CancellationToken ct)
        {
            if (items.Count() == 0)
            {
                throw new ArgumentException("Is empty", nameof(items));
            }

            if (_checkpointGateway.CanWrite)
            {
                await PersistItemsAsync(_checkpointGateway, items, false, false, ct);
                foreach (var item in items)
                {
                    ProcessNewItem(item);
                }
            }
            else
            {
                await CompactAsync(ct);
                await PersistNewItemsAsync(items, ct);
            }
        }

        private void ProcessNewItem(StatusItem item)
        {
            _statusIndex.IndexNewItem(item);
            OnStatusChanged();
        }

        private void OnStatusChanged()
        {
            if (StatusChanged != null)
            {
                StatusChanged(this, EventArgs.Empty);
            }
        }

        private static IImmutableList<StatusItem> ParseCsv(byte[] buffer)
        {
            const bool VALIDATE_HEADER = true;

            if (!buffer.Any())
            {
                return ImmutableArray<StatusItem>.Empty;
            }
            else
            {
                using (var stream = new MemoryStream(buffer))
                using (var reader = new StreamReader(stream))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    if (VALIDATE_HEADER)
                    {
                        csv.Read();
                        csv.ReadHeader();
                        csv.ValidateHeader<StatusItem>();
                    }
                    var items = csv.GetRecords<StatusItem>();

                    return items.ToImmutableArray();
                }
            }
        }

        private static async Task PersistItemsAsync(
            CheckpointGateway checkpointGateway,
            IEnumerable<StatusItem> items,
            bool persistHeaders,
            bool canWriteInMultipleBatch,
            CancellationToken ct)
        {
            const long MAX_LENGTH = 4000000;

            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                if (persistHeaders)
                {
                    csv.WriteHeader<StatusItem>();
                    csv.NextRecord();
                }

                foreach (var item in items)
                {
                    var positionBefore = stream.Position;

                    csv.WriteRecord(item);
                    csv.NextRecord();
                    csv.Flush();
                    writer.Flush();

                    var positionAfter = stream.Position;

                    if (positionAfter > MAX_LENGTH)
                    {
                        if (canWriteInMultipleBatch)
                        {
                            stream.SetLength(positionBefore);
                            stream.Flush();
                            await checkpointGateway.WriteAsync(stream.ToArray(), ct);
                            stream.SetLength(0);
                            csv.WriteRecord(item);
                            csv.NextRecord();
                        }
                        else
                        {
                            throw new NotSupportedException("Too much data to write at once");
                        }
                    }
                }
                csv.Flush();
                writer.Flush();
                if (stream.Position > 0)
                {
                    stream.Flush();
                    await checkpointGateway.WriteAsync(stream.ToArray(), ct);
                }
            }
        }

        private async Task CompactAsync(CancellationToken ct)
        {
            var tempCheckpointGateway =
                await _checkpointGateway.GetTemporaryCheckpointGatewayAsync(ct);

            await PersistItemsAsync(tempCheckpointGateway, _statusIndex.AllItems, true, true, ct);
            _checkpointGateway = await tempCheckpointGateway.MoveOutOfTemporaryAsync(ct);
        }
    }
}