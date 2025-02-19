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
        /// <summary>Used for all three types of blobs:  index, log (shard) & view.</summary>
        private class BlobHeader
        {
            public Version AppVersion { get; set; } = new();
        }

        /// <summary>Used by the index blob.</summary>>
        private class IndexInfo
        {
            public long ShardCount { get; set; } = 0;
        }

        /// <summary>Used by log blobs (shards).</summary>
        private class LogInfo
        {
            public long ShardIndex { get; set; } = 0;

            public bool IsNewProcess { get; set; } = true;
        }

        /// <summary>Used by view blobs.</summary>
        private class ViewInfo
        {
            public long LastShardIncluded { get; set; } = 0;
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
        private bool _isNewProcess = true;
        private long _currentShardIndex;
        private IAppendStorage2 _logAppendStorage;
        private int _appendCount = 0;

        #region Constructors
        private LogStorage(
            IFileSystem fileSystem,
            Version appVersion,
            long currentShardIndex,
            IAppendStorage2 logAppendStorage)
        {
            _fileSystem = fileSystem;
            _appVersion = appVersion;
            _currentShardIndex = currentShardIndex;
            _logAppendStorage = logAppendStorage;
        }

        public async static Task<LogStorage> CreateAsync(
            IFileSystem fileSystem,
            Version appVersion,
            CancellationToken ct)
        {
            var logFileCount = await FetchShardCountAsync(fileSystem, ct);
            var currentShardIndex = logFileCount + 1;
            var logAppendStorage = await GetLogAppendStorageAsync(
                fileSystem,
                currentShardIndex,
                ct);

            return new LogStorage(fileSystem, appVersion, currentShardIndex, logAppendStorage);
        }

        private async static Task<long> FetchShardCountAsync(
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
                    using (var reader = new StreamReader(stream))
                    {
                        var headerText = await reader.ReadLineAsync();
                        var indexText = await reader.ReadLineAsync();

                        var header = headerText != null
                            ? JsonSerializer.Deserialize<BlobHeader>(headerText)
                            : null;
                        var indexInfo = indexText != null
                            ? JsonSerializer.Deserialize<IndexInfo>(indexText)
                            : null;

                        if (header == null)
                        {
                            throw new InvalidDataException("Index blob doesn't contain header");
                        }
                        if (indexInfo == null)
                        {
                            throw new InvalidDataException(
                                "Index blob doesn't contain index information");
                        }

                        return indexInfo.ShardCount;
                    }
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
            long shardIndexIncluded = 0;

            //  Start with the view itself
            using (var latestStream = await _fileSystem.OpenReadAsync(LATEST_PATH, ct))
            {
                if (latestStream != null)
                {
                    using (var reader = new StreamReader(latestStream))
                    {
                        var headerText = await reader.ReadLineAsync();
                        var viewText = await reader.ReadLineAsync();

                        var header = headerText != null
                            ? JsonSerializer.Deserialize<BlobHeader>(headerText)
                            : null;
                        var viewInfo = viewText != null
                            ? JsonSerializer.Deserialize<ViewInfo>(viewText)
                            : null;

                        if (header == null)
                        {
                            throw new InvalidDataException("Latest view blob doesn't contain header");
                        }
                        if (viewInfo == null)
                        {
                            throw new InvalidDataException(
                                "Latest view blob doesn't contain view information");
                        }
                        shardIndexIncluded = viewInfo.LastShardIncluded;

                        yield return new BlobChunk(true, latestStream);
                    }
                }
            }
            //  Loop through log files not included in the view
            for (long i = shardIndexIncluded + 1; i < _currentShardIndex; ++i)
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

                        yield return new BlobChunk(logInfo.IsNewProcess, logStream);
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
            var completedShards = await SealShardAsync(ct);

            await AtomicReplaceAsync(
                LATEST_PATH,
                async tempStorage =>
                {
                    using (var memoryStream = new MemoryStream())
                    {   //  Headers for the view
                        var header = new BlobHeader { AppVersion = _appVersion };
                        var viewInfo = new ViewInfo
                        {
                            LastShardIncluded = completedShards
                        };

                        JsonSerializer.Serialize(memoryStream, header, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        JsonSerializer.Serialize(memoryStream, viewInfo, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        await tempStorage.AtomicAppendAsync(memoryStream.ToArray(), ct);
                    }
                    var appendBlock = new List<byte>(MaxBufferSize);

                    //  Content of the view
                    foreach (var character in content)
                    {
                        appendBlock.Add(character);
                        if (appendBlock.Count == MaxBufferSize)
                        {
                            await tempStorage.AtomicAppendAsync(appendBlock, ct);
                            appendBlock.Clear();
                        }
                    }
                    //  Push the remainder of content
                    if (appendBlock.Any())
                    {
                        await tempStorage.AtomicAppendAsync(appendBlock, ct);
                    }
                },
                ct);
        }

        /// <summary>Append some content atomically.</summary>
        /// <param name="content"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task AtomicAppendAsync(IEnumerable<byte> content, CancellationToken ct)
        {
            if (_appendCount == 0)
            {   //  Shard hasn't been initialized
                await InitNewShardAsync(ct);
            }
            if (await _logAppendStorage.AtomicAppendAsync(content, ct))
            {   //  Append worked, ack
                ++_appendCount;
            }
            else
            {   //  Roll over and retry
                await SealShardAsync(ct);
                await AtomicAppendAsync(content, ct);
            }
        }

        #region Shard management
        private async Task<long> SealShardAsync(CancellationToken ct)
        {
            if (_appendCount != 0)
            {
                ++_currentShardIndex;
                _appendCount = 0;
                _logAppendStorage = await GetLogAppendStorageAsync(
                    _fileSystem,
                    _currentShardIndex,
                    ct);
            }

            return _currentShardIndex;
        }

        private async Task InitNewShardAsync(CancellationToken ct)
        {
            await UpdateIndexAsync(ct);
            await AppendHeadersAsync(ct);
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
                        var indexInfo = new IndexInfo { ShardCount = _currentShardIndex };

                        JsonSerializer.Serialize(memoryStream, header, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        JsonSerializer.Serialize(memoryStream, indexInfo, JSON_SERIALIZER_OPTIONS);
                        memoryStream.WriteByte((byte)'\n');
                        await tempStorage.AtomicAppendAsync(memoryStream.ToArray(), ct);
                    }
                },
                ct);
        }

        private async Task AppendHeadersAsync(CancellationToken ct)
        {
            using (var memoryStream = new MemoryStream())
            {
                var header = new BlobHeader { AppVersion = _appVersion };
                var logInfo = new LogInfo
                {
                    IsNewProcess = _isNewProcess,
                    ShardIndex = _currentShardIndex
                };

                JsonSerializer.Serialize(memoryStream, header, JSON_SERIALIZER_OPTIONS);
                memoryStream.WriteByte((byte)'\n');
                JsonSerializer.Serialize(memoryStream, logInfo, JSON_SERIALIZER_OPTIONS);
                memoryStream.WriteByte((byte)'\n');
                await _logAppendStorage.AtomicAppendAsync(memoryStream.ToArray(), ct);
                _isNewProcess = false;
                ++_appendCount;
            }
        }
        #endregion

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