# TODO

## Current goal

We have rough "best case" numbers for a large graph in a single node. How much worse is it for a large graph distributed across several nodes?

Then, let's introduce write acknowledgements and/or read repair, and we can test snapshot windows when nodes may be at different store times.

At some point we need to introduce intersections, lists, and reverse lookups, though, to ensure we're not "over-fitting" to check.

## Core

- [~] Routing abstraction (base). Two-layer routing: one before ID is known, one after.
- [~] Shard assignment (for sharding hydration). Can start with static assignments maybe?
- [ ] Hydration protocol: COPY SQL, replication slot by lease.
- [~] Graph dispatch by shard. (should be straight forward with routing layer)
- [~] Routing should not be calculated per tuple in a subjectset b/c their might be a high cardinality of tuples. We would want to scatter-gather: group all the subjects by route and dispatch concurrently (or up to some concurrency threshold).
- [ ] Server API for graph
- [ ] Graph wiring for remote dispatch
- [ ] Schema updates, tied to storetime / proper schema API that takes into account windows. If you know about a relevant schema update it must be taken into account even if tuple hasn't changed. Max window before schema update must see old schema.
- [ ] Intersection operator – complicates dispatch. Note "BatchCheck" is really "CheckAny" or "CheckUnion". I thnk we need a CheckSomething that takes into account the whole "set equation" – then we dispatch accordingly.
- [ ] ListSubjects
- [ ] Reverse indexing (ListObjects)
- [ ] Read repair (waiting for replication when needed)
- [ ] Abort on unsatisfiable snapshot window. Panicing for now.
- [ ] Write acknowledgements (pre shard movement & rebalancing).
- [ ] Shard movement (w/ write acknowledgements).
- [ ] Lease protocol for shard assignment.
- [ ] GC old undo entries
- [ ] If a graph gets a query that doesn't have tuples for it any more (i.e. they moved), make sure it reroutes without constraining window

## Advanced

- [ ] Rebalance (that still works with concurrent write acknowledgements).
- [ ] Retry w/ replication wait on unsatisfiable snapshot window

## Improvements / Fixes

- Current design assumes a transaction is only ever removing or adding a single tuple with no predicates. We would want a write to be more of a "transaction script" which described a predicate + many tuples to add + many tuples to remove. This is especially important for the change capture structure but obviously the store write interface as well. I just see the store write interface as something which could be replaced with something else, whereas the change stream as a vital part of the protocol relatively agnostic to _how_ writes happen.
- Graph state is updated concurrently during traversal – we will need to review everything exhaustively to ensure there are no race conditions causing invalid state to be considered.
- Package struture is probably not right – no clear core domain package, and nothing is internal
- More Observers & Probes – can we structure them so we can get stats like graph walk depth tables?
- ✅ In scatter-gather dispatch, if one userset errors, we may want to keep going in case another userset is a hit
- The sentinel storedelta value (meaning "from the beginning") needs more protections
- When searching subject sets, eager filter if our subject type is not a type of the subject in the set (e.g. we are searching for a folder but the set type is user; don't bother)
- Empty checks should probably return error rather than undefined window OR we should ALSO include the "outter" window in the signature only for this case
- I don't like the "With...." pattern when constructing structs.
- We updated the snapshot choice to be causal down to the tuple in checks, but not when traversing subject sets (group#member tuples). However, if we're ultimately doing a check, could we propogate that result somehow? Like if we are checking for user reader of a tag, and we check 10 tags, as of the latest tag change, if one responds with an answer with an older snapshot, could we lower the resulting snapshot window of the whole query to the lowest where that subject set tuple did not change? As in, if we know access comes from tag:5, and tag 5 was added 50 snapshots ago, could we determine the end min is 50 snapshots ago?