using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class FileVersionRowItem : RowItemBase
    {
        public Version FileVersion { get; set; } = new Version();

        public override void Validate()
        {
        }
    }
}