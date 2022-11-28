using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    public enum StatusItemState
    {
        Initial = 1,
        Planned = 2,
        Exported = 3,
        Staged = 4,
        Moved = 5,
        Complete = 6,
        Deleted = 7
    }
}