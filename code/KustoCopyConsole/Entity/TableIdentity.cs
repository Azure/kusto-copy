﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    public record TableIdentity(
        Uri ClusterUri,
        string DatabaseName,
        string TableName)
    {
        public override string ToString()
        {
            return $"{{Cluster:'{ClusterUri}', Database:'{DatabaseName}', Table:'{TableName}')";
        }
    }
}