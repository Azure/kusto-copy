using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class SourceTableTest
    {
        [Fact]
        public void CreateTableAndIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var sourceTable = new TableIdentity(new Uri("https://mycluster.com"), "MyDb", "MyTable");
            var iterationId = 1;
            var state = SourceTableState.Planning;

            cache.AppendItem(new SourceTableRowItem
            {
                State = state,
                IterationId = iterationId,
                SourceTable = sourceTable
            });

            Assert.Single(cache.SourceTableMap);
            Assert.Equal(sourceTable, cache.SourceTableMap.Keys.First());
            Assert.Single(cache.SourceTableMap[sourceTable].IterationMap);
            Assert.Equal(
                iterationId,
                cache.SourceTableMap[sourceTable].IterationMap.Keys.First());
            Assert.Equal(
                state,
                cache.SourceTableMap[sourceTable].IterationMap[iterationId].RowItem.State);
        }
    }
}