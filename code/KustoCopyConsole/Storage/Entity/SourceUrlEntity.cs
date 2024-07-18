using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage.Entity
{
    internal class SourceUrlEntity : IterationEntityBase
    {
        public SourceUrlState State { get; }

        public string TableName { get; }

        public int BlockId { get; }

        public Uri BlobUri { get; }
    }
}