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
        Initial,
        Planned,
        Exported,
        Staged,
        Moved,
        Done,
        Deleted
    }
}