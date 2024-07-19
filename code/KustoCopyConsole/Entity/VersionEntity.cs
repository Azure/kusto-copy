using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal class VersionEntity : IRowItemSerializable
    {
        public VersionEntity(Version fileVersion)
        {
            FileVersion = fileVersion;
        }

        public Version FileVersion { get; }

        RowItem IRowItemSerializable.Serialize()
        {
            return new RowItem
            {
                FileVersion = FileVersion.ToString()
            };
        }
    }
}
