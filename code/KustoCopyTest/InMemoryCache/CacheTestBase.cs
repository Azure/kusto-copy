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
        protected const string ACTIVITY_NAME = "my-activity";
        protected readonly TableIdentity SOURCE_TABLE_IDENTITY =
            new(new Uri("https://mycluster.com"), "MyDb", "MyTable");
        protected readonly TableIdentity DESTINATION_TABLE_IDENTITY =
            new(new Uri("https://myothercluster.com"), "MyOtherDb", "MyTable");
    }
}