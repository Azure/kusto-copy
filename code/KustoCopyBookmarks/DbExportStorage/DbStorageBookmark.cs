using Azure.Core;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.DbExportStorage
{
    public class DbStorageBookmark
    {
        #region Inner Types
        private class DbAggregate
        {
            public IImmutableList<DbIterationData>? DbIterations { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly ConcurrentDictionary<bool, ConcurrentDictionary<DateTime, ConcurrentDictionary<int, bool>>>
            _iterationIndex;

        public static async Task<DbStorageBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<DbAggregate>();
            var dbIterationValues = aggregates
                .Where(a => a.Value.DbIterations != null);
            var dbIterations = dbIterationValues
                .Select(v => v.Value.DbIterations!)
                .SelectMany(i => i);

            return new DbStorageBookmark(bookmarkGateway, dbIterations);
        }

        private DbStorageBookmark(BookmarkGateway bookmarkGateway, IEnumerable<DbIterationData> dbIterations)
        {
            _bookmarkGateway = bookmarkGateway;

            var groups = dbIterations
                .GroupBy(i => (i.IsBackfill, i.EpochStartTime))
                .GroupBy(g => g.Key.IsBackfill);
            var index = groups.ToConcurrentDictionary(
                g => g.Key,
                g => g.ToConcurrentDictionary(
                    g2 => g2.Key.EpochStartTime,
                    g2 => g2.ToConcurrentDictionary(i => i.Iteration, i => true)));

            _iterationIndex = index;
            //  Ensures both true and false backfill are already there
            _iterationIndex.TryAdd(true, new ConcurrentDictionary<DateTime, ConcurrentDictionary<int, bool>>());
            _iterationIndex.TryAdd(false, new ConcurrentDictionary<DateTime, ConcurrentDictionary<int, bool>>());
        }

        public async Task EnsureIterationExistsAsync(
            bool isBackfill,
            DateTime epochStartTime,
            int iteration)
        {
            var epochMap = _iterationIndex[isBackfill];

            if (epochMap == null)
            {
                throw new InvalidOperationException(
                    "Both with and without backfill should be part of index");
            }
            else
            {
                if(!epochMap.TryGetValue(epochStartTime, out var iterationMap))
                {
                    epochMap.TryAdd(epochStartTime, new ConcurrentDictionary<int, bool>());
                    //  Maybe a concurrent thread got there before us
                    iterationMap = epochMap[epochStartTime];

                    if (iterationMap == null)
                    {
                        throw new InvalidOperationException(
                            "Iteration bag should be present by now");
                    }
                }

                var didNotExist = iterationMap.TryAdd(iteration, true);

                if (didNotExist)
                {
                    await CreateIterationAsync(new DbIterationData
                    {
                        IsBackfill = isBackfill,
                        EpochStartTime = epochStartTime,
                        Iteration = iteration
                    });
                }
            }
        }

        private async Task CreateIterationAsync(DbIterationData dbIterationData)
        {
            var dbIterationBuffer = SerializationHelper.ToMemory(
                new DbAggregate
                {
                    DbIterations = ImmutableArray<DbIterationData>.Empty.Add(dbIterationData)
                });
            var transaction = new BookmarkTransaction(
                new[] { dbIterationBuffer },
                null,
                null);
            
            await _bookmarkGateway.ApplyTransactionAsync(transaction);
        }
    }
}