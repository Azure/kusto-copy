﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class DbIterationData
    {
        /// <summary>Identify the epoch.</summary>
        public string EpochEndCursor { get; set; } = string.Empty;

        public DateTime? MinIngestionTime { get; set; }

        public DateTime? MaxIngestionTime { get; set; }
    }
}