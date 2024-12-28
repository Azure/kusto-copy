using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using YamlDotNet.Core.Tokens;

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

        [Fact]
        public void UpdateEmptyIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var sourceTable = new TableIdentity(new Uri("https://mycluster.com"), "MyDb", "MyTable");
            var iterationId = 1;
            var state1 = SourceTableState.Planning;
            var state2 = SourceTableState.Planned;

            cache.AppendItem(new SourceTableRowItem
            {
                State = state1,
                IterationId = iterationId,
                SourceTable = sourceTable
            });

            Assert.Equal(
                state1,
                cache.SourceTableMap[sourceTable].IterationMap[iterationId].RowItem.State);

            //  Update
            cache.AppendItem(new SourceTableRowItem
            {
                State = state2,
                IterationId = iterationId,
                SourceTable = sourceTable
            });

            Assert.Equal(
                state2,
                cache.SourceTableMap[sourceTable].IterationMap[iterationId].RowItem.State);
        }

        [Fact]
        public void UpdateIterationWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var sourceTable = new TableIdentity(new Uri("https://mycluster.com"), "MyDb", "MyTable");
            var iterationId = 1;
            var iterationState1 = SourceTableState.Planning;
            var iterationState2 = SourceTableState.Planned;
            var blockId = 1;

            cache.AppendItem(new SourceTableRowItem
            {
                State = iterationState1,
                IterationId = iterationId,
                SourceTable = sourceTable
            });
            cache.AppendItem(new SourceBlockRowItem
            {
                State = SourceBlockState.Planned,
                IterationId = iterationId,
                SourceTable = sourceTable,
                BlockId = blockId
            });

            Assert.Equal(
                iterationState1,
                cache.SourceTableMap[sourceTable].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.SourceTableMap[sourceTable].IterationMap[iterationId].BlockMap);

            //  Update
            cache.AppendItem(new SourceTableRowItem
            {
                State = iterationState2,
                IterationId = iterationId,
                SourceTable = sourceTable
            });

            Assert.Equal(
                iterationState2,
                cache.SourceTableMap[sourceTable].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.SourceTableMap[sourceTable].IterationMap[iterationId].BlockMap);
        }
    }
}