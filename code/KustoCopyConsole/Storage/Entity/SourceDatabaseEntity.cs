﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Storage.Entity.State;

namespace KustoCopyConsole.Storage.Entity
{
    internal class SourceDatabaseEntity : IterationEntityBase
    {
        public SourceDatabaseState State { get; }

        /// <summary>This should be empty for-and-only-for the first iteration.</summary>
        public string CursorStart { get; }
        
        public string CursorEnd { get; }
    }
}