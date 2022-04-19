using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Bookmarks.DbExportStorage
{
    public class DbIterationData
    {
        public bool IsBackfill { get; set; } = false;
        
        public DateTime EpochStartTime { get; set; } = DateTime.MinValue;
        
        public int Iteration { get; set; } = 0;
    }
}