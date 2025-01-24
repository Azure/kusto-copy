using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class UrlTest : CacheTestBase
    {
        [Fact]
        public void DeleteUrl()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var blockId = 1;
            var urlItem1 = new UrlRowItem
            {
                State = UrlState.Exported,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId,
                Url = "https://mystorage.com/myblob.parquet",
                RowCount = 12
            };
            var urlItem2 = new UrlRowItem
            {
                State = UrlState.Exported,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId,
                Url = "https://mystorage.com/myblob-2.parquet",
                RowCount = 125
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
            cache = cache.AppendItem(new BlockRowItem
            {
                State = BlockState.Planned,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId
            });
            cache = cache.AppendItem(urlItem1);
            cache = cache.AppendItem(urlItem2);

            Assert.Equal(
                2,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].UrlMap.Count);

            //  Remove item1
            cache = cache.AppendItem(urlItem1.ChangeState(UrlState.Deleted));

            Assert.Single(
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].UrlMap);
            Assert.Equal(
                urlItem2.RowCount,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].UrlMap.Values.First().RowItem.RowCount);
        }
    }
}