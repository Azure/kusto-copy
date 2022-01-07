using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.ExportStorage
{
    internal class DbIterationStorageData
    {
        public int Iteration { get; set; } = 0;

        public bool AllTablesExported { get; set; } = false;
    }
}