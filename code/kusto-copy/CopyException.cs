﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kusto_copy
{
    public class CopyException : Exception
    {
        public CopyException(string message, Exception? innerException = null)
            : base(message, innerException)
        {
        }
    }
}
