using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    public interface IStagingBlobUriProvider
    {
        Task<IEnumerable<Uri>> GetWritableRootUrisAsync(string path, CancellationToken ct);

        Task<Uri> AuthorizeUriAsync(Uri uri, CancellationToken ct);
    }
}