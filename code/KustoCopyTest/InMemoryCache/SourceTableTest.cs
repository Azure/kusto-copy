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
            var state = TableState.Planning;

            cache.AppendItem(new IterationRowItem
            {
                State = state,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });

            Assert.Single(cache.ActivityMap);
            Assert.Equal(SOURCE_TABLE_IDENTITY, cache.ActivityMap.Keys.First());
            Assert.Single(cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap);
            Assert.Equal(
                iterationId,
                cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap.Keys.First());
            Assert.Equal(
                state,
                cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
        }

        [Fact]
        public void UpdateEmptyIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = TableState.Planning;
            var state2 = TableState.Planned;

            cache.AppendItem(new IterationRowItem
            {
                State = state1,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });

            Assert.Equal(
                state1,
                cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);

            //  Update
            cache.AppendItem(new IterationRowItem
            {
                State = state2,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });

            Assert.Equal(
                state2,
                cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
        }

        [Fact]
        public void UpdateIterationWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var iterationState1 = TableState.Planning;
            var iterationState2 = TableState.Planned;
            var blockId = 1;

            cache.AppendItem(new IterationRowItem
            {
                State = iterationState1,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });
            cache.AppendItem(new BlockRowItem
            {
                State = BlockState.Planned,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY,
                BlockId = blockId
            });

            Assert.Equal(
                iterationState1,
                cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap);

            //  Update
            cache.AppendItem(new IterationRowItem
            {
                State = iterationState2,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });

            Assert.Equal(
                iterationState2,
                cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.ActivityMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap);
        }
    }
}