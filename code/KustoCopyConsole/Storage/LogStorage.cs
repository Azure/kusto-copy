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
            public long LastLogFileIndexIncluded { get; set; } = 0;
        }

        private class LogInfo
        {
            public long LogFileIndex { get; set; } = 0;

            public bool IsFirstLogInProcess { get; set; } = true;
        }
        #endregion

        private const string INDEX_PATH = "logs/index.log";
        private const string LATEST_PATH = "logs/latest.log";
        private const string HISTORICAL_LOG_ROOT_PATH = "logs/historical/";
        private const string TEMP_PATH = "logs/temp/";

        private static readonly JsonSerializerOptions JSON_SERIALIZER_OPTIONS = new()
        {
            WriteIndented = false,
            PropertyNameCaseInsensitive = true
        };

        private readonly IFileSystem _fileSystem;
        private readonly Version _appVersion;
        private long _currentLogFileIndex;
        private IAppendStorage2 _logAppendStorage;
        private int _appendCount = 0;
        private bool _isFirstLogInProcess = true;

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
        public int MaxBufferSize => _logAppendStorage.MaxBufferSize;

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
                    logFileIndexIncluded = viewInfo.LastLogFileIndexIncluded;

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
            await AtomicReplaceAsync(
                LATEST_PATH,
                async tempStorage =>
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        var header = new BlobHeader { AppVersion = _appVersion };
                        var viewInfo = new ViewInfo
                        {
                            LastLogFileIndexIncluded = _currentLogFileIndex
                        };

                        JsonSerializer.Serialize(memoryStream, header, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        JsonSerializer.Serialize(memoryStream, viewInfo, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        await tempStorage.AtomicAppendAsync(memoryStream.ToArray(), ct);
                    }
                    var appendBlock = new List<byte>(MaxBufferSize);

                    foreach (var character in content)
                    {
                        appendBlock.Add(character);
                        if(appendBlock.Count > MaxBufferSize)
                        {
                            await tempStorage.AtomicAppendAsync(appendBlock, ct);
                            appendBlock.Clear();
                        }
                    }
                    if (appendBlock.Any())
                    {
                        await tempStorage.AtomicAppendAsync(appendBlock, ct);
                    }
                },
                ct);
            if (_appendCount != 0)
            {
                await RollOverAppendStorageAsync(ct);
            }
        }

        /// <summary>Append some content atomically.</summary>
        /// <param name="content"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task AtomicAppendAsync(IEnumerable<byte> content, CancellationToken ct)
        {
            if (_appendCount == 0)
            {
                if (_isFirstLogInProcess)
                {
                    await UpdateIndexAsync(ct);
                }
                using (var memoryStream = new MemoryStream())
                {
                    var header = new BlobHeader { AppVersion = _appVersion };
                    var logInfo = new LogInfo
                    {
                        IsFirstLogInProcess = _isFirstLogInProcess,
                        LogFileIndex = _currentLogFileIndex
                    };

                    JsonSerializer.Serialize(memoryStream, header, JSON_SERIALIZER_OPTIONS);
                    memoryStream.WriteByte((byte)'\n');
                    JsonSerializer.Serialize(memoryStream, logInfo, JSON_SERIALIZER_OPTIONS);
                    memoryStream.WriteByte((byte)'\n');
                    await _logAppendStorage.AtomicAppendAsync(memoryStream.ToArray(), ct);
                    _isFirstLogInProcess = false;
                    ++_appendCount;
                }
            }
            if (await _logAppendStorage.AtomicAppendAsync(content, ct))
            {   //  Append worked, ack
                ++_appendCount;
            }
            else
            {   //  Roll over and retry
                await RollOverAppendStorageAsync(ct);
                await AtomicAppendAsync(content, ct);
            }
        }

        private async Task RollOverAppendStorageAsync(CancellationToken ct)
        {
            ++_currentLogFileIndex;
            await UpdateIndexAsync(ct);
            _logAppendStorage = await GetLogAppendStorageAsync(
                _fileSystem,
                _currentLogFileIndex,
                ct);
            _appendCount = 0;
        }

        private async Task UpdateIndexAsync(CancellationToken ct)
        {
            await AtomicReplaceAsync(
                INDEX_PATH,
                async tempStorage =>
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        var header = new BlobHeader { AppVersion = _appVersion };
                        var indexInfo = new IndexInfo { LogFileCount = _currentLogFileIndex };

                        JsonSerializer.Serialize(memoryStream, header, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        JsonSerializer.Serialize(memoryStream, indexInfo, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        await tempStorage.AtomicAppendAsync(memoryStream.ToArray(), ct);
                    }
                },
                ct);
        }

        private async Task AtomicReplaceAsync(
            string path,
            Func<IAppendStorage2, Task> appendTempStorageFunc,
            CancellationToken ct)
        {
            var tempFileName = $"{TEMP_PATH}{Guid.NewGuid()}.log";
            var tempStorage = await _fileSystem.OpenWriteAsync(tempFileName, ct);

            await appendTempStorageFunc(tempStorage);

            await _fileSystem.MoveAsync(tempFileName, path, ct);
            await _fileSystem.RemoveFolderAsync(TEMP_PATH, ct);
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