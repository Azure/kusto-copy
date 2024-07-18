using CsvHelper.Configuration.Attributes;
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
        public DateTime Created { get; set; } = DateTime.Now;

        [Index(1)]
        public DateTime Updated { get; set; } = DateTime.Now;

        [Index(2)]
        public string Version { get; set; } = string.Empty;
    }
}