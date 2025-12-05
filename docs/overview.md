# Design Overview (draft)

> NOTE: This may yet contain some mistakes, or the design itself may have flaws.

We are building a Zanzibar-style authorization system optimized for "modest infrastructure" (e.g., Kubernetes with standard RAM limits).

**The Core Hypothesis:**
By decoupling **Identity** (Strings) from **Graph Structure** (Integers), we can fit massive ACL graphs (500M+ tuples) entirely in RAM. By relying on **Postgres Logical Replication** as the single source of truth for time, we achieve Snapshot Isolation without complex distributed clocks.

**Key Constraints:**
* **Scale:** 100s of millions of tuples.
* **Workload:** Read-heavy.
* **Consistency:** Snapshot Isolation is mandatory. We prefer waiting (latency) over inconsistency (Frankenstates).
* **Simplicity:** We avoid "Consistency Tokens" in the public API. The system negotiates consistency automatically.

---

## 2. Core Data Model

To achieve the necessary density, we separate the system into two layers: The **Directory** (Identity) and the **Engine** (Graph).

### 2.1 The Integer Core (The Engine)
String IDs are memory-prohibitive. We map every external ID to a dense `uint32`.
* **Storage:** The graph is stored as `AdjacencyList<uint32, RoaringBitmap>`.
* **Efficiency:** Roaring Bitmaps compress sets of integers extremely efficiently.
* **Constraint:** This requires a mapping layer.

### 2.2 The Directory (Identity Map)
* **Role:** Maps `String <-> uint32`.
* **Implementation:** Postgres Table (`mappings`) + Distributed Cache (e.g., Groupcache) on the Router nodes.
* **Lifecycle:** Immutable. Once an ID is assigned, it never changes.

### 2.3 Sharding Topology
We use three layers of abstraction to manage locality:
1.  **Shard Root:** The logical grouping (e.g., "Project:123"). All data for a root should ideally live together.
2.  **Virtual Shard:** A fixed bucket (e.g., 1024 shards). `Hash(Root) % 1024`.
3.  **Node:** A physical pod. Owns a set of Virtual Shards.

---

## 3. Architecture Components

### 3.1 The Router (Stateless)
* **Ingress:** Receives API requests.
* **Translation:** Resolves Strings to Integers (via Cache/DB).
* **Routing:** Determines the Shard Owner for the Subject/Object.
* **Orchestration:** Initiates the distributed pointer chase.

### 3.2 The Worker (Stateful)
* **Role:** Holds the In-Memory Graph.
* **Input:** Consumes the **Postgres Logical Replication Stream**.
* **State:**
    * **Replicated LSN:** The highest LSN processed from the log.
    * **Object State:** The data, managed via MVCC (Head + Delta Chain).

### 3.3 The Store (Postgres)
* **Source of Truth:** Durable storage.
* **The Clock:** The WAL provides the global ordering (LSN).
* **The Bus:** Logical Replication Slots distribute writes to Workers.

---

## 4. Consistency Model: The "Min/Max LSN" Protocol

Because we cannot reliably get the `CommitLSN` synchronously during a write, we rely on the **Replication Stream** to drive our in-memory state.

### 4.1 The Write Path
1.  **Write:** Client writes to Postgres. Transaction Commits.
2.  **Wait (Optional):** If the client requires immediate Read-Your-Writes, we can wait until the replication stream catches up to the flush position.
3.  **Ingest:** Workers receive the update via the WAL.
4.  **Update:** Worker updates RAM, creating a new MVCC entry.

### 4.2 The Read Path (Distributed Snapshot Isolation)

To avoid "Frankenstates" (inconsistent views across shards), we use a negotiation protocol during traversal.

**The Variables:**
* **`Node.ReplicatedLSN`:** The point in time this Node has caught up to.
* **`Query.MaxLSN`:** The upper bound. We *cannot* read newer than this (to ensure we don't see futures relative to our start).
* **`Query.MinLSN`:** The lower bound. We *must* read at least this fresh (to ensure causality).

**The Protocol:**
1.  **Start:** Router picks an initial worker (based on shard). First worker picks LSN range.
    * *Option A (Linearizable, if writes wait for replication):* `MinLSN = Object.LSN` `MaxLSN = Node.ReplicatedLSN`.
    * *Option B (Linearizable always, but slower):* `MinLSN = DB.LastFlushedLSN` (Likely requires some waiting).
2.  **Traversal (Hop):**
    * We request Object X.
    * **Constraint Check:**
        * If `Node.ReplicatedLSN < Query.MinLSN`: **Wait** (Read Repair). The node is too stale to satisfy the causal dependency.
        * If `Node.ReplicatedLSN >= Query.MinLSN`: Proceed.
3.  **Snapshot Selection:**
    * The Node picks the latest version of Object X such that `Object.LSN <= Query.MaxLSN`.
    * **Feedback:**
        * If the chosen version has `LSN > Query.MinLSN`, we bump `Query.MinLSN` for subsequent hops. This ensures that if we see a "new" parent, we don't subsequently look for an "old" child that doesn't exist yet.
        * We effectively narrow the window `[Min, Max]` as we traverse.

### 4.3 Why this works
* **No Aborts:** We prefer waiting (Read Repair) over aborting.
  * The only situation we abort (and possibly retry) is if our range is too far in the past. In this case, no amount of waiting for the future will recover the past, so we cannot continue. We can possibly continue query evaluation to figure out a new minimum range, and restart with minimal waiting.
* **No Tokens:** The system discovers the necessary constraints as it walks the graph.
* **Safety:** We never return data that violates the Snapshot constraint relative to the start of the query.

---

## 5. Lifecycle & Rebalancing

Handling new pods or restarting pods is critical for the "In-Memory" assumption.

### 5.1 Startup (Zero-Gap Hydration)
A new pod uses **Snapshot-Synchronized Replication** to boot.

1.  **Slot Creation:** Pod creates a replication slot. Postgres returns a `SnapshotID` and `SlotLSN`.
2.  **Hydration:** Pod executes `SELECT * FROM data SET TRANSACTION SNAPSHOT 'SnapshotID'`.
    * This loads the graph *exactly* as it existed at `SlotLSN`.
3.  **Streaming:** Pod consumes the WAL from the slot.
    * Because the slot starts exactly where the snapshot ended, there is no data loss and no gap.
    * The pod builds up its MVCC history locally during this catch-up phase.

### 5.2 Rebalancing
* **Consistent Hashing:** Limits shard movement.
* **Lazy Handover:**
    * Old Owner: Mark "Draining". Continue serving reads until New Owner is ready.
    * New Owner: Hydrate in background. Mark "Ready" when WAL lag is near zero.
    * Router: Switch traffic when New Owner is Ready.
