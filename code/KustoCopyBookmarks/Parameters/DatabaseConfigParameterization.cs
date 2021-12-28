﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Parameters
{
    public class DatabaseConfigParameterization
    {
        public string Name { get; set; } = string.Empty;

        public string? DestinationName { get; set; } = null;

        public bool? IsEnabled { get; set; } = null;

        public int? SubIterationRowCount { get; set; } = null;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as DatabaseConfigParameterization;

            return other != null
                && object.Equals(Name, other.Name)
                && object.Equals(DestinationName, other.DestinationName)
                && object.Equals(IsEnabled, other.IsEnabled)
                && object.Equals(SubIterationRowCount, other.SubIterationRowCount);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}