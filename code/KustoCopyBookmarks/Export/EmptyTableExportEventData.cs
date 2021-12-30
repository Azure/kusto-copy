﻿using KustoCopyBookmarks.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class EmptyTableExportEventData
    {
        /// <summary>Identify the epoch.</summary>
        public string EpochEndCursor { get; set; } = string.Empty;

        /// <summary>Identify the iteration.</summary>
        public int Iteration { get; set; } = 0;

        /// <summary>Identify the table.</summary>
        public string TableName { get; set; } = string.Empty;

        public TableSchemaData? Schema { get; set; }
    }
}