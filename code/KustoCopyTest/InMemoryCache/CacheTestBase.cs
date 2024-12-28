using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyTest.InMemoryCache
{
    public class CacheTestBase
    {
        protected readonly TableIdentity TABLE_IDENTITY =
            new(new Uri("https://mycluster.com"), "MyDb", "MyTable");
    }
}