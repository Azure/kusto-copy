using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage.LocalDisk
{
    internal class LocalAppendStorage : IAppendStorage
    {
        private readonly string _path;
        private Stream _fileStream;

        public LocalAppendStorage(string path)
        {
            _path = path;
            _fileStream = OpenFile(_path);
        }

        int IAppendStorage.MaxBufferSize => 4096;

        async Task<byte[]> IAppendStorage.LoadAllAsync(CancellationToken ct)
        {
            await _fileStream.DisposeAsync();

            var buffer = await File.ReadAllBytesAsync(_path, ct);

            _fileStream = OpenFile(_path);

            return buffer;
        }

        async Task IAppendStorage.AtomicReplaceAsync(byte[] buffer, CancellationToken ct)
        {
            var tempPath = Path.GetTempFileName();

            await File.WriteAllBytesAsync(tempPath, buffer, ct);
            await _fileStream.DisposeAsync();
            File.Move(tempPath, _path, true);
            _fileStream = OpenFile(_path);
        }

        async Task<bool> IAppendStorage.AtomicAppendAsync(byte[] buffer, CancellationToken ct)
        {
            await _fileStream.WriteAsync(buffer);

            return true;
        }

        private Stream OpenFile(string path)
        {
            return new FileStream(
                path,
                FileMode.Append,
                FileAccess.Write,
                FileShare.None,
                0,
                FileOptions.WriteThrough | FileOptions.Asynchronous);
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _fileStream.DisposeAsync();
        }
    }
}
