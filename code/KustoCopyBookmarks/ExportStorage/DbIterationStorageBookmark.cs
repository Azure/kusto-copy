using Azure.Core;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.ExportStorage
{
    public class DbIterationStorageBookmark
    {
        #region Inner Types
        private class IterationAggregate
        {
            public DbIterationStorageData? DbIterationStorage { get; set; }
            
            public TableStorageData? TableStorageData { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private readonly BookmarkBlockValue<DbIterationStorageData>? _dbIteration;
        private readonly List<BookmarkBlockValue<TableStorageData>> _tables;

        public static async Task<DbIterationStorageBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<IterationAggregate>();
            var dbIterations = aggregates
                .Where(a => a.Value.DbIterationStorage != null);
            var tables = aggregates
                .Where(a => a.Value.TableStorageData != null);

            if (dbIterations.Count() > 1)
            {
                throw new InvalidOperationException(
                    "Expected at one db iteration definition block");
            }

            return new DbIterationStorageBookmark(
                bookmarkGateway,
                dbIterations.Select(b => b.Project(a => a.DbIterationStorage!)).FirstOrDefault(),
                tables.Select(b => b.Project(a => a.TableStorageData!)));
        }

        private DbIterationStorageBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<DbIterationStorageData>? dbIteration,
            IEnumerable<BookmarkBlockValue<TableStorageData>> tables)
        {
            _bookmarkGateway = bookmarkGateway;
            _dbIteration = dbIteration;
            _tables = tables.ToList();
        }
    }
}