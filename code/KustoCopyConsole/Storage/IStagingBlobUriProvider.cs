using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    public interface IStagingBlobUriProvider
    {
        Task<Uri> FetchUriAsync(CancellationToken ct);

        Task<Uri> AuthorizeUriAsync(Uri uri, CancellationToken ct);
    }
}