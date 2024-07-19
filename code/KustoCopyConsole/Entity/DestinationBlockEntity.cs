﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Entity
{
    internal class DestinationBlockEntity : DestinationDatabaseEntityBase
    {
        public DestinationBlockEntity()
        {
            throw new NotImplementedException();
        }

        public DestinationBlockState State { get; }

        public int BlockId { get; }
        
        public string TempTableName { get; }
        
        public string OperationId { get; }
    }
}