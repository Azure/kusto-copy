using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyFoundation;
using KustoCopyFoundation.Bookmarks;
using KustoCopyFoundation.Concurrency;
using KustoCopySpecific.Parameters;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Bookmarks.DbStorage
{
    public class DbStorageBookmark
    {
        #region Inner Types
        private class DbStorageAggregate
        {
            public CompleteDatabaseParameterization? DbConfig { get; set; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        private BookmarkBlockValue<CompleteDatabaseParameterization> _dbConfig;

        #region Constructors
        public static async Task<DbStorageBookmark> RetrieveAsync(
            DataLakeFileClient fileClient,
            TokenCredential credential,
            CompleteDatabaseParameterization dbConfig)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(fileClient, credential, false);
            var aggregates = await bookmarkGateway.ReadAllBlockValuesAsync<DbStorageAggregate>();
            var dbConfigValue = aggregates
                .Where(a => a.Value.DbConfig != null)
                .Select(a => a.Project(i => i.DbConfig))
                .Cast<BookmarkBlockValue<CompleteDatabaseParameterization>>()
                .FirstOrDefault();

            if (dbConfigValue == null)
            {
                var dbConfigBuffer = SerializationHelper.ToMemory(dbConfig);

                var transaction = new BookmarkTransaction(
                    new[] { dbConfigBuffer },
                    null,
                    null);
                var result = await bookmarkGateway.ApplyTransactionAsync(transaction);

                dbConfigValue = new BookmarkBlockValue<CompleteDatabaseParameterization>(
                    result.AddedBlockIds.First(),
                    dbConfigValue!.Value);
            }

            return new DbStorageBookmark(bookmarkGateway, dbConfigValue);
        }

        private DbStorageBookmark(
            BookmarkGateway bookmarkGateway,
            BookmarkBlockValue<CompleteDatabaseParameterization> dbConfig)
        {
            _bookmarkGateway = bookmarkGateway;
            _dbConfig = dbConfig;
        }
        #endregion

        #region DbParameterization
        public CompleteDatabaseParameterization DbParameterization { get => _dbConfig.Value; }
        #endregion

        #region DbEpochData
        public CompleteDatabaseParameterization DbParameterization { get => _dbConfig.Value; }
        #endregion
    }
}