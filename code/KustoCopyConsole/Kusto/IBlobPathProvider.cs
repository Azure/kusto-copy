using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    public interface IBlobPathProvider
    {
        Task<Uri> FetchUriAsync(CancellationToken ct);

        Task<Uri> AuthorizeUriAsync(Uri uri, CancellationToken ct);
    }
}