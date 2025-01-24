using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class BlockTest : CacheTestBase
    {
        [Fact]
        public void UpdateEmptyBlock()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = BlockState.Planned;
            var state2 = BlockState.Exporting;
            var blockId = 1;
            var item = new BlockRowItem
            {
                State = state1,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId
            };

            cache = cache.AppendItem(new ActivityRowItem
            {
                State = ActivityState.Active,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            });
            cache = cache.AppendItem(new IterationRowItem
            {
                State = IterationState.Starting,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                CursorEnd = "ABC"
            });
            cache = cache.AppendItem(item);

            Assert.Equal(
                state1,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].RowItem.State);

            //  Update
            cache = cache.AppendItem(item.ChangeState(state2));

            Assert.Equal(
                state2,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].RowItem.State);
        }

        [Fact]
        public void UpdateBlockWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = BlockState.Planned;
            var state2 = BlockState.Exporting;
            var blockId = 1;
            var item = new BlockRowItem
            {
                State = state1,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId
            };

            cache = cache.AppendItem(new ActivityRowItem
            {
                State = ActivityState.Active,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            });
            cache = cache.AppendItem(new IterationRowItem
            {
                State = IterationState.Starting,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                CursorEnd = "ABC"
            });
            cache = cache.AppendItem(item);
            cache = cache.AppendItem(new UrlRowItem
            {
                State = UrlState.Exported,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId,
                Url = "https://mystorage.com/myblob.parquet",
                RowCount = 12
            });

            Assert.Equal(
                state1,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].RowItem.State);

            //  Update
            cache = cache.AppendItem(item.ChangeState(state2));

            Assert.Equal(
                state2,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].RowItem.State);
            Assert.Single(cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].UrlMap);
        }
    }
}