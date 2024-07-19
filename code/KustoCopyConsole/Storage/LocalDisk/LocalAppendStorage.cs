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
        //using (FileStream stream = new FileStream(journalFilePath, FileMode.Append, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))

        Task<bool> IAppendStorage.AtomicAppendAsync(byte[] buffer, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        Task IAppendStorage.AtomicReplaceAsync(byte[] buffer, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        byte[] IAppendStorage.LoadAllAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
