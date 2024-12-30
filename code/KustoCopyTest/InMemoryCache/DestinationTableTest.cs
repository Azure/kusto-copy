using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;

namespace KustoCopyTest.InMemoryCache
{
    public class DestinationTableTest : CacheTestBase
    {
        [Fact]
        public void CreateTableAndIteration()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var sourceState = SourceTableState.Planning;
            var destinationState = DestinationTableState.TempTableCreating;

            cache.AppendItem(new SourceTableRowItem
            {
                State = sourceState,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });
            cache.AppendItem(new DestinationTableRowItem
            {
                State = destinationState,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY,
                IterationId = iterationId,
                TempTableName = "MyTemptable"
            });

            Assert.Single(cache.SourceTableMap);
            Assert.Equal(SOURCE_TABLE_IDENTITY, cache.SourceTableMap.Keys.First());
            Assert.Single(cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap);
            Assert.Equal(
                iterationId,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap.Keys.First());
            Assert.Equal(
                sourceState,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].RowItem.State);
            Assert.Single(
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap);
            Assert.Equal(
                DESTINATION_TABLE_IDENTITY,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap.Keys.First());
        }

        [Fact]
        public void UpdateIterationWithChildren()
        {
            var cache = new RowItemInMemoryCache(Array.Empty<RowItemBase>());
            var iterationId = 1;
            var state1 = DestinationTableState.TempTableCreating;
            var state2 = DestinationTableState.TempTableCreated;
            var blockId = 1;
            var item = new DestinationTableRowItem
            {
                State = state1,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY,
                IterationId = iterationId,
                TempTableName = "MyTemp"
            };

            cache.AppendItem(new SourceTableRowItem
            {
                State = SourceTableState.Planned,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY
            });
            cache.AppendItem(new SourceBlockRowItem
            {
                State = SourceBlockState.Planned,
                IterationId = iterationId,
                SourceTable = SOURCE_TABLE_IDENTITY,
                BlockId = blockId
            });
            cache.AppendItem(item);
            cache.AppendItem(new DestinationBlockRowItem
            {
                State = DestinationBlockState.Queuing,
                SourceTable = SOURCE_TABLE_IDENTITY,
                DestinationTable = DESTINATION_TABLE_IDENTITY,
                IterationId = iterationId,
                BlockId = blockId
            });

            Assert.Equal(
                state1,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap[DESTINATION_TABLE_IDENTITY].RowItem.State);
            Assert.Single(cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap[DESTINATION_TABLE_IDENTITY].BlockMap);

            //  Update
            cache.AppendItem(item.ChangeState(state2));

            Assert.Equal(
                state2,
                cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap[DESTINATION_TABLE_IDENTITY].RowItem.State);
            Assert.Single(cache.SourceTableMap[SOURCE_TABLE_IDENTITY].IterationMap[iterationId].DestinationMap[DESTINATION_TABLE_IDENTITY].BlockMap);
        }
    }
}