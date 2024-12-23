using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal abstract class RowItemBase
    {
        public DateTime Created { get; set; } = DateTime.UtcNow;

        public DateTime Updated { get; set; } = DateTime.UtcNow;

        public abstract void Validate();

        public RowItemBase Clone()
        {
            var clone = (RowItemBase)MemberwiseClone();

            clone.Updated = DateTime.UtcNow;

            return clone;
        }
    }
}