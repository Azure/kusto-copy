using KustoCopyConsole.Entity.RowItems.Keys;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal interface IStagingBlobUriProvider
    {
        Task<IEnumerable<Uri>> GetWritableFolderUrisAsync(
            BlockKey blockKey,
            CancellationToken ct);

        Task<Uri> AuthorizeUriAsync(Uri uri, CancellationToken ct);

        Task DeleteStagingDirectoryAsync(IterationKey iterationKey, CancellationToken ct);
    }
}