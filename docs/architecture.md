# Architecture

Falcon is a distributed in memory graph of relationships with tunable consistency, inspired by Google Zanzibar and SpiceDB.

SpiceDB-style operations are exposed externally:

- check(object, relation, subject)
- list_objects(object_type, relation, subject)
- list_subjects(object, relation, subject_type)
- write_tuples(mutations, preconditions)

Internally, a lot happens that is unique to Falcon. This is how Falcon works at a high level:

## How Falcon Works

### Writes & replication

- Writes go straight to an underlying postgres store, in a serializable transaction, dependent on precondition queries being satisifed.
- New objects get assigned a "shard root" based on their relationships and schema. (e.g. a document's shard root might be its folder). Absent a relationship for a shard root, an object is its own root.
- New objects get internal, 32 bit integer IDs, to support dense compression in roaring bitmaps (close to run-length encoding). A SIEVE/LRU cache bi-maps these external and internal IDs (the "ext/int cache"). These IDs are provisioned sequentially by shard root. For roots themselves, they use a common "null root" or "global root" (0). IDs are stored 64 bits with the high 32 bits identifying the shard root and the low 32 bits the ID within that root.
- Nodes configure their own replication slots, and listen to changes in the store. Changes are tracked with their associated LSN. History is kept using deltas by LSN. This is the primary way that nodes learn about state changes to the graph as well as to the cluster. When a replication slot is established, a full hydration is done against that snapshot (which includes the LSN), for gap-less replication.
- Write changes can be "acknowledged," by waiting for replication of a particular "marked" (WAL message, or other mechanism) transaction, on one or more nodes in the cluster (the primary owner of a shard, all owners of a shard, or the entire cluster). The view of the cluster for the sake of write acknowledgement is retrieved from the DB, within the same transaction as the write. If this view is different from the last known view seen from the WAL, additional nodes may be asked to ack up to the latest cluster view (see Rebalancing).
- Each node is assigned shards with a consistent hash algorithm. Shards for objects are assigned according to their "root." This balances locality with distribution. When a new node starts, it hydrates using a consistent snapshot read, and continues from the WAL at that point.
- History is kept up to a point. After that point, deltas are removed. The latest state is always kept. The cutoff may be LSN based or quantity based or both.
- Objects may move between shard roots. When this happens, a new ID is assigned, under the new root. Old IDs are not reused[^1]. This affects the external/internal ID bi-map. When a root changes, this goes into the replication stream, like everything else. Caches update from this. Graphs also update from this. This includes all related tuples, which hydrates the new node, and updates other nodes' pointers (if any). The current owner tracks the new ID, and the LSN at which it changed (a kind of tombstone). When a node reserves a query for an object which moved, it forwards the request if the LSN window includes the new ID. At some point, this tombstone is garbage collected. At that point, we can either wait for a cluster-wide ack up to the tombstone's LSN (likely cheap), or accept the risk that some queries may fail if they still contain the old pointer. A similar failure can occur if the old object ID is not in the ext/int cache for a reverse query.

[^1]: It could be possible to reuse these, to keep integer sequences dense, but it's complicated. For example, maybe we could wait after a certain period of time in which it is practically impossible that any node could have the old ID still.

### Queries

- Queries are distributed throughout nodes based on shards. To achieve causal consistency (snapshot isolation), queries use a consistent LSN (ignoring later state changes). The effective LSN is lazily determined by gradually narrowing the snapshot window as the query traverses the graph. Constraints are tunable per query.
- During replication, we track two kinds of LSNs:
  - Replicated LSN: The last LSN we've seen from log. This is shared for the whole process. This represents the point in the log that we know our in memory state is up to date with.
  - State LSN: The LSN for a particular object state. We may have many of these per object, to support MVCC. This lets us pick states and/or points in the log with which to serve reads from a consistent snapshot.
- The goal of this protocol is enable fast reads with serializable isolation by fitting the entire graph in memory across shards and picking an LSN which we can meet (a) without knowing up front what position in the WAL nodes are, (b) is as high as possible without waiting on read repair, and (c) is as low as possible without having to abort and retry. The only objects examined are those that are needed to solve the query.
  - Two (possibly three) LSN values are included in requests and responses between pointer hops across the query (this happens regardless of whether the hop is within the same node or not): 
    1. A maximum LSN. This is the maximum LSN we would like to be considered. The value comes from our Replicated LSN. It is what we are sure we are up to already, and therefore don't need to wait for.
    2. A minimum requested LSN. This is the minimum LSN we would like to be considered. The value comes from the **latest** state of the tuple which we are traversing, no higher than the maximum LSN.
      - NOTE: We can go further back in the snapshots of a set if it does not change the answer.
    3. [Optional, more advanced, not sure if worth it] A minimum historical LSN. This would be as far back as we could go for the query without aborting. This could be use to retry the query at an _earlier_ snapshot.
  - When receiving a query, we determine an LSN range for the query. If LSN range is specified (which it always is if coming from a peer node), we constrain our range to that under these rules.
    - We try to serve the latest object state no greater than the maximum LSN. We bump our own minimum LSN if we pick a state GREATER than the minimum LSN. You can serve an object state LESSER than the minimum LSN, but the minimum LSN never decreases; it is max(state lsn, minimum LSN).
    - We set the new maximum LSN to min(maximum LSN, replicated LSN).
    - If our replicated (maximum LSN) is less than the queries MIN LSN, then we wait to replicate up to the min LSN and answer the query based on that state.
    - If we are up to date and we have no object state less than or equal to the maximum LSN, it means the caller is far behind. We have two options, based on the preference of the caller. If the caller is okay waiting, we continue the query with both the min and max = ~oldest object state LSN we have, ONLY returning as little as possible to determine a new reachable LSN (maybe we still have to return all the bitmaps, but if we can avoid it, they will not be used in the final result). When the query finishes evaluating, the caller gets a new LSN, which it must now wait for. We start the process from there (picking a new object LSN accordingly, if it was updated). If the caller does not want to wait, we abort at the first point we cannot meet their constraint.

So you get:

- Linearizability if any of:
  1. You pick a time window pinned on "last flushed lsn" (nodes must all wait if not already caught up)
  2. You wait for writes to be acknowledged by shards and you wait for max causally relevant snapshot on read
  3. You wait for writes to be acknowledge everywhere
- Monotonic reads if any of:
  1. There are no secondary replicas for a shard.
  - If there are secondaries, then you can read from node 1, get max 100, then read from node 2, get max 99
  2. You use any of the techniques to get linearizability (above)
- Causal consistency, always

### Rebalancing

- Rebalancing occurs when nodes are administratively added or removed from an existing cluster. We attempt to move as little data around as possible.
- When adding a node, the new node gets assigned shards, and it hydrates. After hydration, a new cluster view is written, which is replicated via the WAL. At this point, some nodes might know to route to it, some might not.
- Nodes keep their own internal view of whether or not they know their peers are at up to date with the latest cluster view, as they know it. When they learn of a new cluster view, this peer state resets. Nodes learn of a new cluster view from both acknowledged writes as well as the WAL. This state is updated as writes are acknolwedged, during rebalance read repair, or when tuples are garbaged collected (more details later). Peer state is "in doubt" when we are unsure if a peer has as up to date cluster view as us.
- During shard-level write acknolwedgements, if any peer state is "in doubt," we upgrade the write ack to include those in-doubt peers. It may be possible to optimize this further[^2]. The tracking of peer state is itself an optimization to prevent us from requiring a cluster level ack for every acknowledged write.
- When a node sees that it owns an object that it didn't know about previously, it sends a peer query to the **immediate previous** owner to hydrate that object ("rebalance read repair"). It goes to a node (and not the DB) in order to get LSN information. That node may recursively call a previous peer if it too didn't know it was an owner. This happens until we hit the most recent "self-aware" owner. In which case, it won't happen again until the next rebalance. Each hop is only to the immediate previous owner in order to prevent gaps in history (which would violate correctness). We distinguish between "net new object that no one could've known about" vs "just new to me" by the WAL content. If it's a net new object, it's not a rebalance, and this section does not apply.
- When a node is about to GC history, it checks if it is still the owner of that object. If it sees it no longer owns it, it *pushes* to the **immediate next** owner and removes the object completely. If it believes it still owns it, it retains at least the latest state of the object. Again, this recurses until a node is satisfied that it is the owner (as far as it knows). This is to avoid the owner missing state and critical LSN information, which it cannot get from the database.

[^2]: I tried to limit acknowledgements to _just_ those peers which previously owned the object(s) in question, however this would probably require we track the cluster epoch for peers, not just a boolean. Then, for each node, we'd check if it owned any object in ANY of the epochs between the last seen and the latest. It's probably doable, and might even save some noticeable time in larger clusters. But this is a rare enough event that I am not spending time on it now.

## Application architecture

### Layer 1: Transport

Package: internal/transport

This is the transport that presents the remotely available operations of Falcon to the wire.

This speaks in terms of low level primitives: protocol specifics like serialization, status codes, and so on.

The gRPC server and proto for the external API live here. There is no business logic here. It invokes the next layer.

Depends on: Layer 2 (Application) and likely Layer 3 (Domain).

### Layer 2: Application

Package: internal/application

This defines the operations that Falcon exposes in terms of protocol agnostic domain objects. Specifically, in terms of value objects (which are or are intended to be immutable). It stitches together high level operations with cross cutting concerns like observability or transaction boundaries. Wire & serialization protocols map to this.

There is little business logic here. It's mainly concerned with cross-cutting "application" level needs like observability, transactions (for database operations)

Depends on: Layer 3 (Domain)

### Layer 3: Domain

Package: internal/domain

This defines the core "business" logic and models (entities and value objects) used in graph resolution, usersets, sharding, ID provisioning, routing, rebalancing, etc. Interfaces are used to encapsulate external infrastructure like peer nodes or databases.

Depends on: n/a (no other layer)

### Layer 4: Infrastructure

Package: internal/infrastructure/{vendor}

These packages are for vendor specific implementations of interfaces specified elsewhere. Packages do not depend on these; they only depend on the interface. Interfaces are usually defined in Layer 3: Domain.

Infrastructure code is usually low-level and optimized for technical details. It usually has minimal business logic, but adheres to the contract defined by the Domain layer.

Depends on: Layer 3 (Domain)