using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class UrlTest : CacheTestBase
    {
        [Fact]
        public void DeleteUrls()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var blockId = 1;
            var blockItem = new BlockRowItem
            {
                State = BlockState.Exported,
                ActivityName = ACTIVITY_NAME,
                IterationId = iterationId,
                BlockId = blockId
            };
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
            cache = cache.AppendItem(blockItem);
            cache = cache.AppendItem(urlItem1);
            cache = cache.AppendItem(urlItem2);

            Assert.Equal(
                2,
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].UrlMap.Count);

            //  Rerturn block to planned state
            blockItem = blockItem.ChangeState( BlockState.Planned);
            cache = cache.AppendItem(blockItem);

            Assert.Empty(
                cache.ActivityMap[ACTIVITY_NAME].IterationMap[iterationId].BlockMap[blockId].UrlMap);
        }
    }
}