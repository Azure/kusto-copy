using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class SourceTableTest : CacheTestBase
    {
        [Fact]
        public void CreateTableAndIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state = SourceTableState.Planning;

            cache.AppendItem(new SourceTableRowItem
            {
                State = state,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY
            });

            Assert.Single(cache.SourceTableMap);
            Assert.Equal(TABLE_IDENTITY, cache.SourceTableMap.Keys.First());
            Assert.Single(cache.SourceTableMap[TABLE_IDENTITY].IterationMap);
            Assert.Equal(
                iterationId,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap.Keys.First());
            Assert.Equal(
                state,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
        }

        [Fact]
        public void UpdateEmptyIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = SourceTableState.Planning;
            var state2 = SourceTableState.Planned;

            cache.AppendItem(new SourceTableRowItem
            {
                State = state1,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY
            });

            Assert.Equal(
                state1,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);

            //  Update
            cache.AppendItem(new SourceTableRowItem
            {
                State = state2,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY
            });

            Assert.Equal(
                state2,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
        }

        [Fact]
        public void UpdateIterationWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var iterationState1 = SourceTableState.Planning;
            var iterationState2 = SourceTableState.Planned;
            var blockId = 1;

            cache.AppendItem(new SourceTableRowItem
            {
                State = iterationState1,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY
            });
            cache.AppendItem(new SourceBlockRowItem
            {
                State = SourceBlockState.Planned,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY,
                BlockId = blockId
            });

            Assert.Equal(
                iterationState1,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].BlockMap);

            //  Update
            cache.AppendItem(new SourceTableRowItem
            {
                State = iterationState2,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY
            });

            Assert.Equal(
                iterationState2,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].BlockMap);
        }
    }
}