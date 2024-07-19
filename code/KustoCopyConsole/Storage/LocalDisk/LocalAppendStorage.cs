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
        private readonly Stream _stream;

        public LocalAppendStorage(string path)
        {
            _path = path;
            _stream = OpenFile(_path);
        }

        async Task<byte[]> IAppendStorage.LoadAllAsync(CancellationToken ct)
        {
            var length = _stream.Length;
            var buffer = new byte[length];

            _stream.Position = 0;

            var readLength = await _stream.ReadAsync(buffer, ct);

            if (readLength != length)
            {
                throw new CopyException($"File '{_path}' corrupted", false);
            }

            return buffer;
        }

        Task IAppendStorage.AtomicReplaceAsync(byte[] buffer, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        Task<bool> IAppendStorage.AtomicAppendAsync(byte[] buffer, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        private Stream OpenFile(string path)
        {
            return new FileStream(
                path,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                4096,
                FileOptions.WriteThrough);
        }
    }
}
