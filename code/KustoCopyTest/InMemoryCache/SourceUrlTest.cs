using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class SourceUrlTest : CacheTestBase
    {
        [Fact]
        public void DeleteUrl()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var urlState1 = UrlState.Exported;
            var urlState2 = UrlState.Deleted;
            var blockId = 1;
            var urlItem1 = new UrlRowItem
            {
                State = urlState1,
                SourceTable = SOURCE_TABLE_IDENTITY,
                IterationId = iterationId,
                BlockId = blockId,
                Url = "https://mystorage.com/myblob.parquet",
                RowCount = 12
            };
            var urlItem2 = new UrlRowItem
            {
                State = urlState1,
                SourceTable = SOURCE_TABLE_IDENTITY,
                IterationId = iterationId,
                BlockId = blockId,
                Url = "https://mystorage.com/myblob-2.parquet",
                RowCount = 125
            };

            cache.AppendItem(new IterationRowItem
            {
                State = TableState.Planning,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });
            cache.AppendItem(new BlockRowItem
            {
                State = BlockState.Exporting,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY,
                BlockId = blockId,
                OperationId = "abc"
            });
            cache.AppendItem(urlItem1);
            cache.AppendItem(urlItem2);

            Assert.Equal(
                2,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].UrlMap.Count);

            //  Remove item1
            cache.AppendItem(urlItem1.ChangeState(urlState2));

            Assert.Single(
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].UrlMap);
            Assert.Equal(
                urlItem2.RowCount,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].UrlMap.Values.First().RowItem.RowCount);
        }
    }
}