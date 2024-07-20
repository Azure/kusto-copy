using CsvHelper.Configuration.Attributes;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal class RowItem
    {
        [Index(0)]
        public string FileVersion { get; set; } = string.Empty;

        [Index(1)]
        public DateTime? Created { get; set; }

        [Index(2)]
        public DateTime? Updated { get; set; }

        [Index(3)]
        public string State { get; set; } = string.Empty;

        [Index(4)]
        public string SourceClusterUri { get; set; } = string.Empty;

        [Index(5)]
        public string SourceDatabaseName { get; set; } = string.Empty;

        [Index(6)]
        public long? IterationId { get; set; }

        [Index(7)]
        public string CursorStart { get; set; } = string.Empty;

        [Index(8)]
        public string CursorEnd { get; set; } = string.Empty;

        [Index(9)]
        public string SourceTableName { get; set; } = string.Empty;
    }
}