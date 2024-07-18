using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage.LocalDisk
{
    internal class LocalItemGateway : IItemGateway
    {
        //using (FileStream stream = new FileStream(journalFilePath, FileMode.Append, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
 
        int IItemGateway.MaxBatchSize => 4086;

        Task IItemGateway.AppendItemsAsync(IEnumerable<RowItem> rowItems)
        {
            throw new NotImplementedException();
        }

        Task<IImmutableList<RowItem>> IItemGateway.LoadAllAsync()
        {
            throw new NotImplementedException();
        }

        Task IItemGateway.ReplaceAllAsync(IEnumerable<RowItem> rowItems)
        {
            throw new NotImplementedException();
        }
    }
}
