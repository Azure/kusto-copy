using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Parameters
{
    public class DatabaseOverrideParameterization
    {
        public bool? IsEnabled { get; set; } = null;

        public string? BackfillHorizon { get; set; }

        public string? Rpo { get; set; }
    }
}