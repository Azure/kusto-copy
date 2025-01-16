using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class ActivityTest : CacheTestBase
    {
        [Fact]
        public void CreateActivity()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var state = ActivityState.Active;

            cache.AppendItem(new ActivityRowItem
            {
                State = state,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            });

            Assert.Single(cache.ActivityMap);
            Assert.Equal(ACTIVITY_NAME, cache.ActivityMap.Keys.First());
            Assert.Equal(state, cache.ActivityMap[ACTIVITY_NAME].RowItem.State);
            Assert.Equal(
                SOURCE_TABLE_IDENTITY,
                cache.ActivityMap[ACTIVITY_NAME].RowItem.SourceTable);
        }

        [Fact]
        public void UpdateEmptyActivity()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var state1 = ActivityState.Active;
            var state2 = ActivityState.Completed;
            var item = new ActivityRowItem
            {
                State = state1,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            };

            cache.AppendItem(item);
            Assert.Equal(state1, cache.ActivityMap.Values.First().RowItem.State);

            //  Update
            item = item.ChangeState(state2);
            cache.AppendItem(item);

            Assert.Equal(state2, cache.ActivityMap.Values.First().RowItem.State);
        }

        [Fact]
        public void UpdateIterationWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var state1 = ActivityState.Active;
            var state2 = ActivityState.Completed;
            var item = new ActivityRowItem
            {
                State = state1,
                ActivityName = ACTIVITY_NAME,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY
            };

            cache.AppendItem(item);
            cache.AppendItem(new IterationRowItem
            {
                State =  IterationState.Starting,
                ActivityName = ACTIVITY_NAME,
                IterationId = 1,
                CursorEnd="ABC"
            });
            Assert.Equal(state1, cache.ActivityMap.Values.First().RowItem.State);
            Assert.Single(cache.ActivityMap.Values.First().IterationMap);

            //  Update
            item = item.ChangeState(state2);
            cache.AppendItem(item);

            Assert.Equal(state2, cache.ActivityMap.Values.First().RowItem.State);
            Assert.Single(cache.ActivityMap.Values.First().IterationMap);
        }
    }
}