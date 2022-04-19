using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyFoundation
{
    internal static class DbNullHelper
    {
        public static T? To<T>(this object obj) where T : struct
        {
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            else
            {
                return (T)obj;
            }
        }
    }
}