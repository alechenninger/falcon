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
- The DependentSet return is probably an optimization most of the time, but if it didn't change the window and was a huge result set, that would be bad. Fortunately, we can fall back to a narrower window without including dependent results if we detect the sets would be too large, even compressed, to transmit over the network. This would probably be quite an expensive check as it is, given it would mean a huge number of sets were checked to be a meaningfully large bitmap (e.g. 10k+?)
- Also re dependent set stuff: is figuring out all these windows pointless if this is the "end" of the query? Not if we ever want to return consistency tokens for "advanced" use cases.
- How would this design deal with partial evaluation of conditionals? I guess it counts as dependent for the sake of causality.

## Thoughts / Maybe bad ideas

- I think I wrote this elsewhere but I think we can potentially allow INCREASES to the MAX so long as the client is comfortable waiting and potentially retrying. We may be able to determine if any of the recent changes would have affected the result and only retry in that case. What are the semantics of this? You get as fresh results as anything in the cluster. But why would it be needed? You can already get linearizable reads with write acknowledgements. I guess it would give you monotonic reads without waiting on write acknowledgements? No, we already get that without secondaries; and with secondaries it doesn't solve the problem (the first node can still be the limiting factor). And would it result it potential recursive retries? This is I think why I had originally envisioned this like "just try to find the snapshot" for the rest of the query. But maybe it would be more elegant to say, "on the first node, use infinite max instead of your max, and wait up to the result max, potentially retrying with a real max after that." How do you reason about this though? Ignore secondaries. What about linearizability with unrelated queries (without write acknolwedgements)? If you make one query that sees up to time 10, and another query that involves that same part of the graph that only considers up to time 5, you might get logical inconsistencies. **This solves that.** ... I think.