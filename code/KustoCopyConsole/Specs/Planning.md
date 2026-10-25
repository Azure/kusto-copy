# Planning

Planning is done in a way that makes it possible to plan an entire history (the first iteration)
that might be year long and millions of extents.  It is done by digging down the time line
by levels.

Levels are:

* 1 day
* 1 hour
* 1 minute
* 1 second
* 100 milliseconds

At each level we create partitions with ingestion_time ranges.  We proceed in a depth-first
manner, i.e. computing the next level for the first partition and so on.

Planning is done differently if `postPlanningKqlQuery` is empty or not, except for the first level.

We define a "time out" in planning as a query taking over 2.5 minutes to complete.  The thinking
behind is that getting close to the default 4 minutes timeout runs the risk of the export query
timing out because the cluster's conditions change.  So we keep this safety margin.

## First Level

`kqlQuery` isn't used at the first level.  We simply breakdown the cursor window into
1-day partitions, without counting the number of rows nor extents.  This makes the query very
cheap but it goes over the entire table for the first iteration.

It has the disadvantage that if the `kqlQuery` is cheaply filtering out a lot of rows
(e.g. time filter), we do not benefitiate from that and will need to query every days to prune
out.  We suppose here that it's more likely to have no time filter by other more expensive filters
(e.g. where EventText has "Error").

## No Post Planning Query

For the second level onwards, we use the `kqlQuery`.  If the query times out or runs out of
memory, we retry with no `kqlQuery` (passing an empty string).  If it fails again, we fail
permanently with a relevant error message.

We take a partition and sub partition it to the next level (e.g. 1 day -> 1 hour), counting
the total number of rows.

We will merge small sub partitions together to avoid small partitions.  We won't merge
sub partitions beyond `MAX_ROW_COUNT_PER_PARTITION` (250 times
`MAX_ROW_COUNT_PER_BLOCK`, which is itself 16M rows).  This is done at the KQL query
level with a `row_cumsum()` function.

###	Creating blocks

If a partition has `MAX_ROW_COUNT_PER_PARTITION` rows or less, we create a block for it.

We do that with 3 queries:

*	One identifies all extents in the partition
*	Identifies the max-created-on time for each extent (this is actually a .show command)
*	Match the rows's block (ingestion_time() ranges) with their extent max-created-on
	and merge the blocks together if the max-created-on is close enough (always keeping the
	biggest value).  Also done with `row_cumsum()` in-query.  Each block should be at most
	`MAX_ROW_COUNT_PER_BLOCK` rows (16M) ; this is best effort but we don't merge beyond that.

We use max-created-on (instead of min) so that latest data win and on the destination we will not
evict data before the source cluster.

We take the resulting blocks and create blocks with those.

For both partitions and blocks, ingestion-time is persisted as string.  This is because a trivia
with the Kusto-SDK where Kusto seems to have higher time resolution on its `datetime` than .NET,
hence keeping .NET `datatime` leads to inconsistencies.

## With Post Planning Query

For each partition, we run a query with both `kqlQuery` and `postPlanningKqlQuery` and count
the number of rows.

If it times outs, runs out of memory or returns more than `MAX_ROW_COUNT_PER_BLOCK`, we subdivide
the partition with the same logic as if `postPlanningKqlQuery` was empty (see previous section).

Otherwise, we create a block for the partition.

## Notes to implementer

It isn't clear how we can detect an out-of-memory condition in a query.  We might need to
test to see what the response looks like and if we can reliably detect it.