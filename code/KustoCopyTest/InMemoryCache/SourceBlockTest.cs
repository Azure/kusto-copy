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
            var iterationState1 = SourceBlockState.Planned;
            var iterationState2 = SourceBlockState.Exporting;
            var blockId = 1;
            var blockItem = new SourceBlockRowItem
            {
                State = iterationState1,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY,
                BlockId = blockId
            };

            cache.AppendItem(new SourceTableRowItem
            {
                State = SourceTableState.Planning,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY
            });
            cache.AppendItem(blockItem);

            Assert.Equal(
                iterationState1,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);

            //  Update
            cache.AppendItem(blockItem.ChangeState(iterationState2));

            Assert.Equal(
                iterationState2,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);
        }

        [Fact]
        public void UpdateBlockWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var iterationState1 = SourceBlockState.Exporting;
            var iterationState2 = SourceBlockState.Exported;
            var blockId = 1;
            var blockItem = new SourceBlockRowItem
            {
                State = iterationState1,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY,
                BlockId = blockId,
                OperationId = "abc"
            };

            cache.AppendItem(new SourceTableRowItem
            {
                State = SourceTableState.Planning,
                IterationId = iterationId,
                SourceTable = TABLE_IDENTITY
            });
            cache.AppendItem(blockItem);
            cache.AppendItem(new SourceUrlRowItem
            {
                State = SourceUrlState.Exported,
                SourceTable = TABLE_IDENTITY,
                IterationId = iterationId,
                BlockId = blockId,
                Url = "https://mystorage.com/myblob.parquet",
                RowCount = 12
            });

            Assert.Equal(
                iterationState1,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);

            //  Update
            cache.AppendItem(blockItem.ChangeState(iterationState2));

            Assert.Equal(
                iterationState2,
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].RowItem.State);
            Assert.Single(
                cache.SourceTableMap[TABLE_IDENTITY].IterationMap[iterationId].BlockMap[blockId].UrlMap);
        }
    }
}