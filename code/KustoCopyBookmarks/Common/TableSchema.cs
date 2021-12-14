using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Common
{
    public class TableSchema
    {
        /// <summary>
        /// In the format col1:type1, col2:type2, etc.
        /// </summary>
        public string Schema { get; set; } = string.Empty;

        public string Folder { get; set; } = string.Empty;
        
        public string DocString { get; set; } = string.Empty;
    }
}