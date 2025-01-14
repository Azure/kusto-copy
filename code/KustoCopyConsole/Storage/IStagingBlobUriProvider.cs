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
        IEnumerable<Uri> GetWritableRootUris(string path);

        Uri AuthorizeUri(Uri uri);
    }
}