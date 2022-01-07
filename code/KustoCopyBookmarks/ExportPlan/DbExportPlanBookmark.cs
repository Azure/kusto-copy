using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Parameters;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Reflection.Metadata;
using System.Text.Json;

namespace KustoCopyBookmarks.ExportPlan
{
    public class DbExportPlanBookmark
    {
        #region Inner Types
        public class DbIterationEventArgs : EventArgs
        {
            public DbIterationEventArgs(
                DbEpochData dbEpoch,
                DbIterationData dbIteration,
                IEnumerable<TableExportPlanData> tablePlans)
            {
                DbEpoch = dbEpoch;
                DbIteration = dbIteration;
                TablePlans = tablePlans.ToImmutableArray();
            }

            public DbEpochData DbEpoch { get; }

            public DbIterationData DbIteration { get; }

            public IImmutableList<TableExportPlanData> TablePlans { get; }
        }

        private class ExportPlanAggregate
        {
            public DbEpochData? DbEpoch { get; set; }

            public DbIterationData? DbIteration { get; set; }

            public TableExportPlanData? TableExportPlan { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly List<BookmarkBlockValue<DbEpochData>> _dbEpochs;
        private readonly List<BookmarkBlockValue<DbIterationData>> _dbIterations;
        private readonly List<BookmarkBlockValue<TableExportPlanData>> _tableExportPlan;

        public event EventHandler<DbIterationEventArgs>? NewDbIteration;

        public static async Task<DbExportPlanBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<ExportPlanAggregate>();
            var epochs = aggregates
                .Where(a => a.Value.DbEpoch != null);
            var dbIterations = aggregates
                .Where(a => a.Value.DbIteration != null);
            var tableExportPlans = aggregates
                .Where(a => a.Value.TableExportPlan != null);

            if (epochs.Count() > 2)
            {
                throw new InvalidOperationException(
                    "Expected at most two iterations definition block");
            }

            return new DbExportPlanBookmark(
                bookmarkGateway,
                epochs.Select(b => b.Project(a => a.DbEpoch!)),
                dbIterations.Select(b => b.Project(a => a.DbIteration!)),
                tableExportPlans.Select(b => b.Project(a => a.TableExportPlan!)));
        }

        private DbExportPlanBookmark(
            BookmarkGateway bookmarkGateway,
            IEnumerable<BookmarkBlockValue<DbEpochData>> dbEpochs,
            IEnumerable<BookmarkBlockValue<DbIterationData>> dbIterations,
            IEnumerable<BookmarkBlockValue<TableExportPlanData>> tableExportPlans)
        {
            _bookmarkGateway = bookmarkGateway;
            _dbEpochs = dbEpochs.ToList();
            _dbIterations = dbIterations.ToList();
            _tableExportPlan = tableExportPlans.ToList();
        }

        public IImmutableList<DbEpochData> GetAllDbEpochs()
        {
            lock (_dbEpochs)
            {
                return _dbEpochs
                    .Select(e => e.Value)
                    .Where(e => !e.AllIterationsExported)
                    .ToImmutableArray();
            }
        }

        public DbEpochData? GetDbEpoch(bool isBackfill)
        {
            var value = GetDbEpochValue(isBackfill);

            return value == null
                ? null
                : value.Value;
        }

        public async Task<DbEpochData> CreateNewEpochAsync(
            bool isBackfill,
            DateTime currentTime,
            string cursor)
        {
            var existingEpoch = GetDbEpochValue(isBackfill);
            var newEpoch = new DbEpochData
            {
                EndCursor = cursor,
                EpochStartTime = currentTime
            };

            if (existingEpoch != null && !existingEpoch.Value.AllIterationsExported)
            {
                throw new InvalidOperationException("There already is an existing epoch");
            }
            if (existingEpoch != null && isBackfill)
            {
                throw new InvalidOperationException("There can be only one backfill epoch");
            }
            if (existingEpoch == null && !isBackfill)
            {
                var existingBackfillEpoch = GetDbEpochValue(true);

                if (existingBackfillEpoch == null)
                {
                    throw new InvalidOperationException(
                        "We can't have a forward epoch before a backfill epoch has been initiated");
                }
                else
                {
                    newEpoch.StartCursor = existingBackfillEpoch.Value.EndCursor;
                }
            }
            var epochBuffer =
                SerializationHelper.ToMemory(new ExportPlanAggregate { DbEpoch = newEpoch });
            var transaction = new BookmarkTransaction(
                new[] { epochBuffer },
                null,
                existingEpoch != null ? new[] { existingEpoch.BlockId } : ImmutableArray<int>.Empty);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);

            lock (_dbEpochs)
            {
                if (existingEpoch != null)
                {
                    _dbEpochs.Remove(existingEpoch);
                }
                _dbEpochs.Add(new BookmarkBlockValue<DbEpochData>(
                    result.AddedBlockIds.First(),
                    newEpoch));

                return newEpoch;
            }
        }

        public IImmutableList<DbIterationData> GetDbIterations(string endCursor)
        {
            lock (_dbIterations)
            {
                var iterations = _dbIterations
                    .Select(i => i.Value)
                    .Where(i => i.EpochEndCursor == endCursor)
                    .ToImmutableArray();

                return iterations;
            }
        }

        public IImmutableList<TableExportPlanData> GetTableExportPlans(
            string endCursor,
            int iteration)
        {
            lock (_tableExportPlan)
            {
                return _tableExportPlan
                    .Select(p => p.Value)
                    .Where(p => p.EpochEndCursor == endCursor)
                    .Where(p => p.Iteration == iteration)
                    .ToImmutableArray();
            }
        }

        public async Task CreateNewDbIterationAsync(
            DbEpochData dbEpoch,
            DbIterationData dbIteration,
            ImmutableArray<TableExportPlanData> tableExportPlans)
        {
            var dbEpochValue = GetDbEpochValue(dbEpoch.IsBackfill);
            var isLastIteration = dbEpoch.IsBackfill
                ? dbIteration.MinIngestionTime == null
                : dbIteration.MaxIngestionTime == null;
            var updatingBlocks = new List<BookmarkBlock>(1);

            if (dbEpochValue == null || dbEpochValue.Value.EndCursor != dbEpoch.EndCursor)
            {
                throw new InvalidOperationException("Invalid db epoch");
            }
            if (isLastIteration)
            {
                dbEpoch.AllIterationsPlanned = true;
                updatingBlocks.Add(new BookmarkBlock(
                    dbEpochValue.BlockId,
                    SerializationHelper.ToMemory(new ExportPlanAggregate { DbEpoch = dbEpoch })));
            }

            var iterationBuffer =
                SerializationHelper.ToMemory(new ExportPlanAggregate { DbIteration = dbIteration });
            var planBuffers = tableExportPlans
                .Select(p => SerializationHelper.ToMemory(new ExportPlanAggregate { TableExportPlan = p }));
            var transaction = new BookmarkTransaction(
                planBuffers.Prepend(iterationBuffer),
                updatingBlocks,
                null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);
            var dbIterationValue = new BookmarkBlockValue<DbIterationData>(
                    result.AddedBlockIds.First(),
                    dbIteration);
            var tableExportPlanValues = result.AddedBlockIds.Skip(1).Zip(
                tableExportPlans,
                (id, p) => new BookmarkBlockValue<TableExportPlanData>(id, p));

            if (isLastIteration)
            {
                lock (_dbEpochs)
                {
                    _dbEpochs.Remove(dbEpochValue);
                    _dbEpochs.Add(new BookmarkBlockValue<DbEpochData>(
                        result.UpdatedBlockIds.First(),
                        dbEpoch));
                }
            }
            lock (_dbIterations)
            {
                _dbIterations.Add(dbIterationValue);
            }
            lock (_tableExportPlan)
            {
                _tableExportPlan.AddRange(tableExportPlanValues);
            }
            OnNewDbIteration(new DbIterationEventArgs(dbEpoch, dbIteration, tableExportPlans));
        }

        public async Task CompleteTableExportPlanAsync(TableExportPlanData tableExportPlan)
        {
            var tableExportPlanValue = GetTableExportPlanValue(
                tableExportPlan.EpochEndCursor,
                tableExportPlan.Iteration,
                tableExportPlan.TableName);

            if (tableExportPlanValue == null)
            {
                throw new InvalidOperationException("Table export plan isn't present anymore");
            }
            var transaction = new BookmarkTransaction(
                null,
                null,
                new[] { tableExportPlanValue.BlockId });
            var result = await _bookmarkGateway.ApplyTransactionAsync(transaction);

            if (result.DeletedBlockIds.Count != 1
                || result.DeletedBlockIds.First() != tableExportPlanValue.BlockId)
            {
                throw new InvalidOperationException(
                    "Inconsistency while deleting table export plan");
            }
        }

        private BookmarkBlockValue<DbEpochData>? GetDbEpochValue(bool isBackfill)
        {
            lock (_dbEpochs)
            {
                return _dbEpochs
                    .Where(e => e.Value.IsBackfill == isBackfill)
                    .FirstOrDefault();
            }
        }

        private BookmarkBlockValue<TableExportPlanData>? GetTableExportPlanValue(
            string epochEndCursor,
            int iteration,
            string tableName)
        {
            lock (_tableExportPlan)
            {
                return _tableExportPlan
                    .Where(t => t.Value.EpochEndCursor == epochEndCursor)
                    .Where(t => t.Value.Iteration == iteration)
                    .Where(t => t.Value.TableName == tableName)
                    .FirstOrDefault();
            }
        }

        private void OnNewDbIteration(DbIterationEventArgs eventArgs)
        {
            if (NewDbIteration != null)
            {
                NewDbIteration(this, eventArgs);
            }
        }
    }
}