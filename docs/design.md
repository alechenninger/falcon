# Design notes

## Napkin math

It should be reasonable to keep an entire, even very large graph (100s of millions of tuples) in memory, if we can distribute the memory across intelligently-allocated shards. This can particularly be helped by using _Roaring Bitmaps_ to very efficiently compress sets of object IDs. 

If the same object occurs in many sets on the same physical node, this can even work with arbitrary IDs. In that case, we keep a mapping of external string IDs to internal integer IDs. The mapping is uncompressed, but it is reused. If sets rarely contain the same object, then "plain" sets are probably more efficient for supporting arbitrary external IDs.

The hypothesis is this kind of sharding is maximally effective if:

- Consistent hashing keeps shard movement down during rebalancing. **Gut check**: fairly well established
- Related sets can be placed in the same shard. **Gut check**: shard by a "root" or "anchor" for each object (such as documents' folder).
- Shards can relatively evenly distribute load. **Gut check**: sharding strategy & data shape with relatively "high" number of relatively "small" shards. Common aggregates in ACL systems may fit this shape: folders, projects, workspaces, namespaces, etc.
- Objects are commonly reused in many sets (amortizing the memory cost of roaring ID mapping). **Gut check**: Not sure. Same user in many groups? Yeah. But can we fit all groups for an org on the same shard? Better locality trades off even load distribution as it is more likely to have more hot shards fight for the same node.
- Graphs are generally wide and shallow. **Gut check**: most ACL systems fit this shape. E.g. a resource hierarchy does not usually have 100 levels of folders. We assume access management systems can set reaonable limits to depth (e.g. 5-10).
- Cross shard writes & shard movement is relatively rare. **Gut check**: Edits within the same shard are easily atomic, fast, & consistent. This design shines in this case. Moving a document between folders is harder, but should be a small fraction of requests compared to reads. Ultimately, this design favors developer experience above all and therefore aims to support linearizability even with cross-shard writes.

Rough numbers:

- 100 million IDs to unit32 mapping takes about 5.6gb. Across 10 nodes that is 560mb each (assuming perfect distribution). Roaring sets in this model can be extremely dense by comparison, because each node populates its own mappings, own sets, with maximally dense integers. To support arbitrary IDs, we need both, but the additional roaring set cost is very small.
- Checking set membership in memory is nanoseconds for low #'s of sets to sub-ms or single-digit ms with higher cardinality (e.g. 1000+ sets).
- Hops between nodes single-digit ms overhead
- Probably the highest cost comes from hops, and syncing delays (see below) – worst case (10 hops, 100s of sets) maybe 50ms for a check?

## 2. Core Concepts

### 2.1 Sharding

To balance granular locality with manageable topology, we define three distinct layers of abstraction:

1. **Shard Root (The Atom):**  
   * **Definition:** The smallest unit of data locality (e.g., a specific Project ID or Repository ID).  
   * **Role:** Unit of **Application Locking** (Mutex) and **Data Co-location**.  
   * **Cardinality:** High (Millions).  
2. **Shard (The Bucket):**  
   * **Definition:** A fixed number of virtual partitions (e.g., 1024 or 4096).  
   * **Mapping:** ShardID \= Hash(ShardRoot) % TotalShards.  
   * **Role:** Unit of **Database Leases** and **Topology Rebalancing**.  
   * **Cardinality:** Fixed (Low Thousands).  
3. **Node (The Host):**  
   * **Definition:** A physical worker pod.  
   * **Mapping:** ShardID \-\> NodeID (Managed by the Controller via Leases).  
   * **Role:** Unit of **Compute** and **RAM**. A single Node owns many Shards.  
   * **Cardinality:** Dynamic (Tens to Hundreds).  
* **Invariant (Steady State):** All objects assigned to a ShardRoot conceptually reside on the same physical worker node.  
  * *Note on Transitions:* During a **Shard Move** (reparenting), an object's physical residency lags behind its logical assignment. The Database is the instant source of truth; the RAM on the Old/New owner converges milliseconds later (via Invalidation/Hydration).

#### 2.1.1. Root Assignment Algorithm

* **Placement Relations:** Strict containment (e.g., parent) determines the Root.
* **Association Relations:** Weak references (e.g., viewer) create cross-shard edges. An object's outbound tuples are colocated with the object's root.

### 2.2. The Shared Store (Postgres)

Postgres acts as:

1. **The WAL:** Durable storage of graph edges.  
2. **The Clock:** Uses **Log Sequence Numbers (LSN)** for global ordering.  
3. **The Event Bus:** Uses **Logical Replication Slots** (pgoutput) to stream ordered updates to workers.
4. **The Directory:** Maps Objects to Shard Roots.


## Techniques-for-semantics:

- Sharding w/ consistent hashing: Distribute data so it fits in memory with minimal changes on rebalance
- MVCC: Keep a rolling snapshot, but deltas by object with LSNs so we can achieve snapshot isolation. This requires logical replication for reverse indexes and cross-write shards. LSNs for primary writes will never be perfect (not possible with postgres), but this should be good enough b/c (a) snapshots can  ...
  - Q: What snapshot do we actually start with/keep?
    - The first object's last edited LSN? – Higher staleness, likely to fall out of MVCC window. Subsequent (different) queries may pick earlier snapshots.
    - The node's last seen LSN? – Very low staleness, likely to have to wait a little somewhere in the query. Subsequent (different) queries may pick earlier snapshots.
    - Somewhere in between (within buffer)? – Still very low staleness, and mathematically best chance of being in nodes' buffers. But arbitrary? Subsequent (different) queries may pick earlier snapshots.
    - Get the current LSN in the db? - Serializable consistency. Likely to have to wait a little somewhere in the query. Subsequent, even different, queries never pick earlier snapshots.
    - Start lower, but retry w/ max? – Appearence of serializability? Which is cheaper – just go fully consistent, or pay for retry?
      - Or even hedge? Do retry-w-max + latest LSN and pick first?
    - Both: provide min lsn (object head, for freshness but minimum freshness) and max verified lsn (from log). We can basically have options:
      - Max consistency without waiting: rely on old deltas, pick the newest we can at the point where all shards are up to date. Avoids "frankenstates" by using an old object a newer LSN would require a node to wait to confirm it did not pick invalid state.
      - Max shard consistency: rely on possible waits (wait until all shards are up to date with the latest object)
      - Linearizable (write optimized / write agnostic): get the most recent flushed LSN at the tip of the query, query at that snapshot (everything waits). This extreme approach is required IF we support cross-shard atomic writes AND we don't want to wait during write.
      - Linearizable (read optimized): Same as "max consistency without waiting" **as long as** any write waited for all nodes involved to acknowledge the write.
  - Queries abort & retry if nodes no longer have the requested snapshot
- Logical replication: Support cross shard writes (reverse indexes, atomic multi-shard association writes, which also require write repair)
  - May also be necessary for snapshot isolation, since its the only way we can get a commit LSN. See below "maybe's" for a possible write optimization.
- Read repair on cache miss for shard owner: if a request comes to a shard which doesn't know it is the new owner yet, it waits for replication (within some timeout?) to see the object.
  - In the synchronous RAM-write model, this was: "Lazy hydration of new object owners: Supports atomic shard moves (owner invalidates, next state must go to new owner)"
- Shard roots: Maintain a dictionary of objects to shard roots, to balance related object locality and shard load balancing. Some objects are their own roots (e.g. medium-sized aggregates like folders, etc.)
- Roaring bitmaps: Very high compression of set membership for high scale.
- Distributed external ID to dense "roaring" integer dictionary: Bi-map external IDs to integers to support arbitrary IDs yet have highly compressed sets.
  - Distribute the mapping across shards (e.g. group cache)
  - Store the mapping in DB (maybe use hash of ID as key, check for collision on write)
  - Tuple store uses integers (faster hydration, denser storage)
- Hydration from snapshot + logical replication on startup: This gives us snapshot isolation during rebalancing.
- Follower nodes: When assigning shards, also assign a follower node which can be used for queries and can quickly become a leader (already hydrated).
  - Q: Can follower nodes use replication shot or do they need to stream from leader to have exact same LSNs?

Maybe's:

- [maybe / possible optimization] Shard owner with lock: In process lock with shard ownership allows (1) a consistent dual-write to disk and RAM and (2) an (optional) follow-up select to get an LSN at-least-as-recent as the last commit (to give us a "max revision" LSN up to date synchronously). Due to imperfect LSN, this may only be an optimization. We cannot rely on that alone for snapshot isolation. We CAN use it as a consistency & latency optimization to immediate get a "minimum LSN" for reads that then hit this shard (getting immediate read-your-write without waiting). However, if a query comes within the "gap" (last verfied object write LSN vs max possible for head state), we'll have to wait for the write's true LSN before knowing what state to use. If a read comes in at ≤ the known write LSN of the object, we can use that state, and then the max outgoing LSN would be our max verified.
  - NOTE: The locking is only really helpful if coupled with "write repair": After writing, read the latest state of an object WITH the LSN, in order to ensure consistent states from cross-shard writes before applying to RAM. Routing to the right shard for writes becomes not a necessity but an optimization (see shard owner with lock). We could optionally get the last flushed LSN and wait for that to replicate, which would not introduce any gaps.
- [I think we only need this if we try to synchronously update RAM as in above bullet] Fencing of writes on shard roots: Ensures writes never populate the wrong shard memory, and shards never miss their writes (even after shard moves) without 2PC
  - Q: Do we still need to fence on every root of every tuple?

Semantics-by-techniques:

- Critical scaling & availability requirements:
  - Roaring bitmaps
  - Global external ID to uint32 mapping
  - Startup hydration protocol
  - Follower nodes
- Snapshot isolation for forward queries: 
  - MVCC (deltas by object)
- Snapshot isolation for reverse queries:
  - Requires MVCC (possibly dual-sided)
  - Logical replication
- Atomic cross-shard writes, so clients do not need to care about sharding and do not need to code for partial writes:
  - Logical replication

## Where to start

### 1.1 Level 0: Graph core

- Roaring bitmaps – graph state is entirely in terms of roaring bitmaps. Assume all reads/writes in terms of uint32 IDs (we'll add the dictionary layer later to support arbitrary external IDs). We can add and check members of sets.

### 1.2 Level 1: Presistence & replication

- Get a write, write to db – no waiting to start.
- Create & tail replication slot, update in memory state with LSNs, keeping history for MVCC. We track two kinds of LSNs:
  - Replicated LSN: The last LSN we've seen from log. This is shared for the whole process. This represents the point in the log that we know our in memory state is up to date with.
  - State LSN: The LSN for a particular object state. We may have many of these per object, to support MVCC. This lets us pick states and/or points in the log with which to serve reads from a consistent snapshot.
- Distributed pointer chasing. The goal of this protocol is enable fast reads with serializable isolation by fitting the entire graph in memory across shards and picking an LSN which we can meet (a) without knowing up front what position in the WAL nodes are, (b) is as high as possible without waiting on read repair, and (c) is as low as possible without having to abort and retry. The only objects examined are those that are needed to solve the query.
  - Two (possibly three) LSN values are included in requests and responses between pointer hops across the query (this happens regardless of whether the hop is within the same node or not): 
    1. A maximum LSN. This is the maximum LSN we would like to be considered. The value comes from our Replicated LSN. It is what we are sure we are up to already, and therefore don't need to wait for.
    2. A minimum requested LSN. This is the minimum LSN we would like to be considered. The value comes from the **latest** state of the tuple which we are traversing, no higher than the maximum LSN.
    3. [Optional, more advanced, not sure if worth it] A minimum historical LSN. This would be as far back as we could go for the query without aborting. This could be use to retry the query at an _earlier_ snapshot.
  - When receiving a query, we determine an LSN range for the query. If LSN range is specified (which it always is if coming from a peer node), we constrain our range to that under these rules.
    - We try to serve the latest object state no greater than the maximum LSN. We bump our own minimum LSN if we pick a state GREATER than the minimum LSN. You can serve an object state LESSER than the minimum LSN, but the minimum LSN never decreases; it is max(state lsn, minimum LSN).
    - We set the new maximum LSN to min(maximum LSN, replicated LSN).
    - IF we have no object state less than or equal to the maximum LSN, we have two options, based on the preference of the caller. If the caller is okay waiting, we continue the query with both the min and max = ~oldest object state LSN we have, ONLY returning as little as possible to determine a new reachable LSN (maybe we still have to return all the bitmaps, but if we can avoid it, they will not be used in the final result). When the query finishes evaluating, we have a new LSN to wait for replication. We start the process from there (picking a new object LSN accordingly, if it was updated). If the caller does not want to wait, we abort at the first point we cannot meet their constraint.

### 1.3 Level 2: Sharding

- Queries are routed based on shard roots
- Replicate is filtered based on shards

### 1.4 Level 3: Read your write (write acknowledgements)

### 1.5 Level 4: External IDs

- Dictionary

## OLD NOTES (may be wrong)

**NOTE**: The clock and event bus are maybe not necessary in the mixed-mode design. It is only necessary to catch cross-shard writes.

I wonder if we can achieve snapshot isolation in mixed mode without replication slots? If we return current LSN for writes and hydrating reads, we know what we are up to. So we can keep deltas per LSN. No! We still need the replication log for eventually consistent reverse indexes.

Aside: Ignoring reverse indexes, is that true for shard movement hydration? All writes we care about must come to us, so we know about those. We might see individual hydrations out-of-order and thus see a later LSN when we still have an older LSN's hydration to do. But, if a query comes for that move, we'll get it. So we're "effectively" up to date by virtue of "knowing" we'll get it eventually.

- If you are routed to an old shard owner (either because your dictionary is cached or because of race conditions between checking the dictionary and changes to the dictionary and your query), that shard may use old state. While traversing other requests may route to the new shard owner for newer state. I guess we have this problem without shard movement: it involves recursing back to the same object, in which case you may see newer state. This particular case is only possible within milliseconds, generally.
  - What about the case where a shard _moved_? A doc could simultaneously be seen as within two folders which would make no sense.
  - I think this is all solved by AT LEAST keeping track of the ancestors of a query, and avoiding those, OR, using snapshot isolation (which, if not using ancestors, requires depth limits to prevent infinite recursion)


LSN selection strategy: Pick lowest as possible; risk replay if state is sufficiently old

Replicated LSN: 20

document:d1 -(parent)> folder:f1 -(readers)> group -(member)> user

latest:     5                    10                20
chosen:     5                    5                 10
oldest:     1                    4                 10
req min:    5                    5                 10
req max:    20                   20                20

If we keep the min for folder readers, we have to replay the request after getting group members, because the minimum is so old that it fell out of group members snapshots, and there are more recent folder readers we have to now consider.

I think we should not just pick the minimum always, because it makes this likely to happen.


LSN selection strategy: Pick latest for an object within request range

Replicated LSN: 20

document:d1 -(parent)> folder:f1 -(readers)> group -(member)> user

latest:     5                    10                20
chosen:     5                    10                10
oldest:     1                    4                 10
req min:    5                    10                10
req max:    20                   20                20

Request served without waiting. Risk is that the range gets more and more constrained as it navigates the graph. But we expect MOST objects to be much older than min, so we start with a low bar most of the time, and it is perfectly fine to serve BELOW that bar, as long as your replicated LSN is ABOVE that bar.