using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal class LogStorage
    {
        #region Inner Types
        private class BlobHeader
        {
            public Version AppVersion { get; set; } = new();
        }

        private class IndexInfo
        {
            public long LogFileCount { get; set; } = 0;
        }

        private class ViewInfo
        {
            public long LogFileIndexIncluded { get; set; } = 0;
        }

        private class LogInfo
        {
            public bool IsFirstLogInProcess { get; set; } = true;
        }
        #endregion

        private const string INDEX_PATH = "logs/index.log";
        private const string LATEST_PATH = "logs/latest.log";
        private const string HISTORICAL_LOG_ROOT_PATH = "logs/historical/";

        private readonly IFileSystem _fileSystem;
        private readonly Version _appVersion;
        private long _currentLogFileIndex;
        private IAppendStorage2 _logAppendStorage;
        private int _appendCount;

        #region Constructors
        private LogStorage(
            IFileSystem fileSystem,
            Version appVersion,
            long currentLogFileIndex,
            IAppendStorage2 logAppendStorage)
        {
            _fileSystem = fileSystem;
            _appVersion = appVersion;
            _currentLogFileIndex = currentLogFileIndex;
            _logAppendStorage = logAppendStorage;
            _appendCount = 0;
        }

        public async static Task<LogStorage> CreateAsync(
            IFileSystem fileSystem,
            Version appVersion,
            CancellationToken ct)
        {
            var logFileCount = await FetchLogFileCountAsync(fileSystem, ct);
            var currentLogFileIndex = logFileCount + 1;
            var logAppendStorage = await GetLogAppendStorageAsync(
                fileSystem,
                currentLogFileIndex,
                ct);

            return new LogStorage(fileSystem, appVersion, currentLogFileIndex, logAppendStorage);
        }

        private async static Task<long> FetchLogFileCountAsync(
            IFileSystem fileSystem,
            CancellationToken ct)
        {
            using (var stream = await fileSystem.OpenReadAsync(INDEX_PATH, ct))
            {
                if (stream == null)
                {
                    return 0;
                }
                else
                {
                    var header = JsonSerializer.Deserialize<BlobHeader>(stream);
                    var indexInfo = JsonSerializer.Deserialize<IndexInfo>(stream);

                    if (header == null)
                    {
                        throw new InvalidDataException("Index blob doesn't contain header");
                    }
                    if (indexInfo == null)
                    {
                        throw new InvalidDataException(
                            "Index blob doesn't contain index information");
                    }

                    return indexInfo.LogFileCount;
                }
            }
        }
        #endregion

        /// <summary>Maximum size of buffer that can be written.</summary>
        public int MaxBufferSize => _fileSystem.MaxBufferSize;

        /// <summary>Reads the latest view by chunks.</summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public async IAsyncEnumerable<BlobChunk> ReadLatestViewAsync(
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            long logFileIndexIncluded = 0;

            //  Start with the view itself
            using (var latestStream = await _fileSystem.OpenReadAsync(LATEST_PATH, ct))
            {
                if (latestStream != null)
                {
                    var header = JsonSerializer.Deserialize<BlobHeader>(latestStream);
                    var viewInfo = JsonSerializer.Deserialize<ViewInfo>(latestStream);

                    if (header == null)
                    {
                        throw new InvalidDataException("Latest view blob doesn't contain header");
                    }
                    if (viewInfo == null)
                    {
                        throw new InvalidDataException(
                            "Latest view blob doesn't contain view information");
                    }
                    logFileIndexIncluded = viewInfo.LogFileIndexIncluded;

                    yield return new BlobChunk(true, latestStream);
                }
            }
            //  Loop through log files not included in the view
            for (long i = logFileIndexIncluded + 1; i < _currentLogFileIndex; ++i)
            {
                using (var logStream = await _fileSystem.OpenReadAsync(GetLogPath(i), ct))
                {
                    if (logStream != null)
                    {
                        var header = JsonSerializer.Deserialize<BlobHeader>(logStream);
                        var logInfo = JsonSerializer.Deserialize<LogInfo>(logStream);

                        if (header == null)
                        {
                            throw new InvalidDataException("Log blob doesn't contain header");
                        }
                        if (logInfo == null)
                        {
                            throw new InvalidDataException(
                                "Log blob doesn't contain view information");
                        }

                        yield return new BlobChunk(logInfo.IsFirstLogInProcess, logStream);
                    }
                }
            }
        }

        /// <summary>Writes the latest view.</summary>
        /// <param name="content"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public async Task WriteLatestViewAsync(IEnumerable<byte> content, CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }

        /// <summary>Append some content atomically.</summary>
        /// <param name="content"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task AtomicAppendAsync(IEnumerable<byte> content, CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }

        private async static Task<IAppendStorage2> GetLogAppendStorageAsync(
            IFileSystem fileSystem,
            long logFileIndex,
            CancellationToken ct)
        {
            return await fileSystem.OpenWriteAsync(GetLogPath(logFileIndex), ct);
        }

        private static string GetLogPath(long logFileIndex)
        {
            return $"{HISTORICAL_LOG_ROOT_PATH}{logFileIndex:D20}.log";
        }
    }
}