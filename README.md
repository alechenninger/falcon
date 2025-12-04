# FalconDb: Distributed, consistent, in-memory ACL graph

Falcon is an experimental project designed to validate an approach to Zanzibar-style ACLs (relationship-based access control) that sacrifices massive, global, Google-scale for a simpler and possibly even faster API that **does not require the client to manage consistency tokens to achieve linearizable reads**. 

Theoretically, it...

- ...solves the "new enemy" and "read your write" problems with no intervention to the data model on the client (linearizable reads)
- ...removes the database from the hot path, simplifying database scaling (such as to support colocation with more data for a centralized inventory).
- ...MAY (TBD) be fast enough to support "pathological" ACL-aware-search (with paging/filtering) cases where often neither pre-filtering nor post-filtering are viable, due to a sufficiently high cardinality of accessible objects that are a sufficiently low percentage of total objects (which typically require "leapord cache" style materialized views)

It is probably NOT better than the traditional, Spanner-based design (e.g. Zanzibar/SpiceDB) for massive graphs (many billions or trillions) or globally consistent services. The question this experiment is inspired by is: do you really need that? And if we sacrifice that, what can we gain in return?
