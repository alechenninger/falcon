# TODO

## Core

- [~] Routing abstraction (base). Two-layer routing: one before ID is known, one after.
- [ ] Shard assignment (for sharding hydration). Can start with static assignments maybe?
- [~] Graph dispatch by shard. (should be straight forward with routing layer)
- [ ] Intersection operator – complicates dispatch
- [ ] Read repair (waiting for replication when needed)
- [ ] Abort on unsatisfiable snapshot window
- [ ] Write acknowledgements (pre shard movement & rebalancing).
- [ ] Shard movement (w/ write acknowledgements).
- [ ] Lease protocol for shard assignment.
- [ ] Schema updates, tied to storetime.
- [ ] GC old undo entries
- [~] Routing should not be calculated per tuple in a subjectset b/c their might be a high cardinality of tuples. We would want to scatter-gather: group all the subjects by route and dispatch concurrently (or up to some concurrency threshold).

## Advanced

- [ ] Rebalance (that still works with concurrent write acknowledgements).
- [ ] Retry w/ replication wait on unsatisfiable snapshot window

## Improvements / Fixes

- Current design assumes a transaction is only ever removing or adding a single tuple with no predicates. We would want a write to be more of a "transaction script" which described a predicate + many tuples to add + many tuples to remove. This is especially important for the change capture structure but obviously the store write interface as well. I just see the store write interface as something which could be replaced with something else, whereas the change stream as a vital part of the protocol relatively agnostic to _how_ writes happen.
- Graph state is updated concurrently during traversal – we will need to review everything exhaustively to ensure there are no race conditions causing invalid state to be considered.
- Package struture is probably not right – no clear core domain package, and nothing is internal
- More Observers & Probes – can we structure them so we can get stats like graph walk depth tables?
- In scatter-gather dispatch, if one userset errors, we may want to keep going in case another userset is a hit