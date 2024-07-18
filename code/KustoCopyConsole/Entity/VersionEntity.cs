using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal class VersionEntity
    {
        public VersionEntity(Version version)
        {
            Version = version;
        }

        public Version Version { get; }
    }
}
