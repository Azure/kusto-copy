using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class SourceBlockTest : CacheTestBase
    {
        [Fact]
        public void UpdateEmptyBlock()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = BlockState.Planned;
            var state2 = BlockState.Exporting;
            var blockId = 1;
            var blockItem = new BlockRowItem
            {
                State = state1,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY,
                BlockId = blockId
            };

            cache.AppendItem(new TableRowItem
            {
                State = TableState.Planning,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });
            cache.AppendItem(blockItem);

            Assert.Equal(
                state1,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);

            //  Update
            cache.AppendItem(blockItem.ChangeState(state2));

            Assert.Equal(
                state2,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);
        }

        [Fact]
        public void UpdateBlockWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = BlockState.Exporting;
            var state2 = BlockState.Exported;
            var blockId = 1;
            var blockItem = new BlockRowItem
            {
                State = state1,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY,
                BlockId = blockId,
                OperationId = "abc"
            };

            cache.AppendItem(new TableRowItem
            {
                State = TableState.Planning,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });
            cache.AppendItem(blockItem);
            cache.AppendItem(new UrlRowItem
            {
                State = UrlState.Exported,
                SourceTable = SOURCE_TABLE_IDENTITY,
                IterationId = iterationId,
                BlockId = blockId,
                Url = "https://mystorage.com/myblob.parquet",
                RowCount = 12
            });

            Assert.Equal(
                state1,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);

            //  Update
            cache.AppendItem(blockItem.ChangeState(state2));

            Assert.Equal(
                state2,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);
            Assert.Single(
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].UrlMap);
        }
    }
}