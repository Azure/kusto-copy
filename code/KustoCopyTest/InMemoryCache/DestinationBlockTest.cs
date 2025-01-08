using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class DestinationBlockTest : CacheTestBase
    {
        [Fact]
        public void UpdateEmptyBlock()
        {
            //  TODO
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = DestinationBlockState.Queuing;
            var state2 = DestinationBlockState.Queued;
            var blockId = 1;
            var sourceBlockItem = new BlockRowItem
            {
                State = BlockState.Exported,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY,
                BlockId = blockId
            };
            var destinationBlockItem = new DestinationBlockRowItem
            {
                State = state1,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY,
                IterationId = iterationId,
                BlockId = blockId
            };

            cache.AppendItem(new TableRowItem
            {
                State = TableState.Planning,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });
            cache.AppendItem(new DestinationTableRowItem
            {
                State = DestinationTableState.TempTableCreating,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY,
                IterationId = iterationId,
                TempTableName = "MyTemp"
            });
            cache.AppendItem(sourceBlockItem);
            cache.AppendItem(destinationBlockItem);

            Assert.Equal(
                state1,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap[DESTINATION_TABLE_IDENTITY].BlockMap[blockId].RowItem.State);

            //  Update
            cache.AppendItem(destinationBlockItem.ChangeState(state2));

            Assert.Equal(
                state2,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap[DESTINATION_TABLE_IDENTITY].BlockMap[blockId].RowItem.State);
        }
    }
}