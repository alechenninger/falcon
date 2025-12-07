# TODO

## Core

## Improvements / Fixes

- Current design assumes a transaction is only ever removing or adding a single tuple with no predicates. We would want a write to be more of a "transaction script" which described a predicate + many tuples to add + many tuples to remove. This is especially important for the change capture structure but obviously the store write interface as well. I just see the store write interface as something which could be replaced with something else, whereas the change stream as a vital part of the protocol relatively agnostic to _how_ writes happen.
- Graph state is updated concurrently during traversal – we will need to review everything exhaustively to ensure there are no race conditions causing invalid state to be considered.
- Package struture is probably not right – no clear core domain package, and nothing is internal
- More Observers & Probes – can we structure them so we can get stats like graph walk depth tables?