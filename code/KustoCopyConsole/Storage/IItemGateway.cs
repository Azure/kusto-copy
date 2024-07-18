using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal interface IItemGateway
    {
        int MaxBatchSize { get; }

        Task<IImmutableList<RowItem>> LoadAllAsync();
 
        Task ReplaceAllAsync(IEnumerable<RowItem> rowItems);

        Task AppendItemsAsync(IEnumerable<RowItem> rowItems);
    }
}