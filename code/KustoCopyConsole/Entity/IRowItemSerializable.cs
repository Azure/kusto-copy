using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal interface IRowItemSerializable
    {
        RowItem Serialize();
    }
}