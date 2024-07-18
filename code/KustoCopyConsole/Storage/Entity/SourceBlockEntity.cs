using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Storage.Entity.State;

namespace KustoCopyConsole.Storage.Entity
{
    internal class SourceUrlEntity : IterationEntityBase
    {
        public SourceBlockState State { get; }

        public string TableName { get; }

        public int BlockId { get; }

        /// <summary>This should be empty for-and-only-for the first block.</summary>
        public string IngestionTimeStart { get; }

        /// <summary>This should be empty for-and-only-for the last block.</summary>
        public string IngestionTimeEnd { get; }

        /// <summary>
        /// This should be non-empty for-and-only-for
        /// <see cref="SourceBlockState.Exporting"/> state.
        /// </summary>
        public string OperationId { get; }
    }
}