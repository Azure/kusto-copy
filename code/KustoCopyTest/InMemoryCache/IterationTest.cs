using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class IterationTest : CacheTestBase
    {
        [Fact]
        public void CreateActivityAndIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state = IterationState.Planning;

            cache.AppendItem(new ActivityRowItem
            {
                State = ActivityState.Active,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            });
            cache.AppendItem(new IterationRowItem
            {
                State = state,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                CursorEnd = "ABC"
            });

            Assert.Single(cache.ActivityMap);
            Assert.Equal(ACTIVITY_NAME, cache.ActivityMap.Keys.First());
            Assert.Single(cache.ActivityMap[ACTIVITY_NAME].IterationMap);
            Assert.Equal(
                iterationId,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap.Keys.First());
            Assert.Equal(
                state,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].RowItem.State);
        }

        [Fact]
        public void UpdateEmptyIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = IterationState.Planning;
            var state2 = IterationState.Planned;
            var item = new IterationRowItem
            {
                State = state1,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                CursorEnd = "ABC"
            };

            cache.AppendItem(new ActivityRowItem
            {
                State = ActivityState.Active,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            });
            cache.AppendItem(item);

            Assert.Equal(
                state1,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].RowItem.State);

            //  Update
            cache.AppendItem(item.ChangeState(state2));

            Assert.Equal(
                state2,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].RowItem.State);
        }

        [Fact]
        public void UpdateIterationWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var iterationState1 = IterationState.Planning;
            var iterationState2 = IterationState.Planned;
            var blockId = 1;
            var item = new IterationRowItem
            {
                State = iterationState1,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                CursorEnd = "ABC"
            };

            cache.AppendItem(new ActivityRowItem
            {
                State = ActivityState.Active,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            });
            cache.AppendItem(item);
            cache.AppendItem(new BlockRowItem
            {
                State = BlockState.Planned,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId
            });

            Assert.Equal(
                iterationState1,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap);

            //  Update
            item = item.ChangeState(iterationState2);
            cache.AppendItem(item);

            Assert.Equal(
                iterationState2,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].RowItem.State);
            Assert.Single(cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap);
        }
    }
}