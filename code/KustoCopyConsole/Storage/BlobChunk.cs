using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal record BlobChunk(bool IsProcessReset, Stream Stream);
}