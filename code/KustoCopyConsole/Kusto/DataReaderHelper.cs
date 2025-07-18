﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal static class DataReaderHelper
    {

        public static IEnumerable<T> ToEnumerable<T>(
            this IDataReader reader,
            Func<IDataReader, T> extractor)
        {
            while (reader.Read())
            {
                yield return extractor(reader);
            }
        }

        public static T ToScalar<T>(this IDataReader reader)
        {
            if (reader.Read())
            {
                return (T)reader.GetValue(0);
            }
            else
            {
                throw new InvalidOperationException("No rows returned.");
            }
        }
    }
}