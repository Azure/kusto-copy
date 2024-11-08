using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal record FileVersionRowItem(Version FileVersion)
        : RowItemBase(DateTime.Now, DateTime.Now)
    {
        public override void Validate()
        {
        }
    }
}