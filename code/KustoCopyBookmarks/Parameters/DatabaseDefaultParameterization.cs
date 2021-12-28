﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Parameters
{
    public class DatabaseDefaultParameterization
    {
        public bool IsEnabled { get; set; } = true;

        public int SubIterationRowCount { get; set; } = 10000000;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as DatabaseDefaultParameterization;

            return other != null
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