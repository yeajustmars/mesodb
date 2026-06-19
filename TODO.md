# GENERAL

1. [ ] Remove NOTES.txt
2. [ ] Remove mentions of ShortTrack/"ST" in any code and/or notes (permanently from all Git history)
3. [ ] Full-text search
4. [ ] Eviction / Excision (GDPR "Right to be Forgotten")
5. [ ] Server-side transaction functions (Lua or WASM scripting in the transactor)
6. [ ] Bitemporal "valid time" vs. "transaction time" separation (currently MesoDB treats them as the same timeline)


# v1 vs v2 backlog

```
Looking strictly at functional parity (ignoring raw performance optimizations for v0.2), here are the four major features that XTDB and/or Datomic possess that are currently missing from the mesodb-core bunker.

1. Reified Transactions (Transaction Metadata)
What it is: In Datomic, a transaction is itself an entity. When you transact data, you can attach facts about the transaction (e.g., who made the change, the HTTP request ID, or the business reason).
Why it matters: Your new History API is incredible for answering "what changed and when?" but without transaction entities, it cannot answer "who changed it and why?"
MesoDB Gap: Right now, your tx_id is just a u64 sequence number. You don't currently have a mechanism to assert a Fact where the EntityId is the tx_id being generated.

2. Excision / Eviction (The GDPR Problem)
What it is: Datomic has excision and XTDB has evict. This is the ability to completely obliterate an entity and all its history from the disk as if it never existed.
Why it matters: Legal compliance (GDPR, CCPA "Right to be Forgotten").
MesoDB Gap: MesoDB is currently a strictly append-only bitemporal ledger. If someone puts a plaintext social security number in the database, retracting it just appends an op: false record. The data still lives forever in the underlying Parquet files and WAL.

3. Server-Side Transaction Functions
What it is: The ability to submit a function to the transactor instead of raw facts. The database executes the function sequentially against the latest database state, and the function yields the facts to be written.
Why it matters: It prevents race conditions for complex logic (e.g., "increment this counter" or "transfer funds if balance > X").
MesoDB Gap: You partially solved this! Your Phase 1 Compare-and-Swap (CAS) implementation (cas_old_v) covers 90% of the optimistic concurrency use cases without needing to embed a scripting language (like Lua or WASM) inside your transactor.

4. Bitemporal "Valid Time" vs. "Transaction Time" Separation
What it is: XTDB distinguishes between tx-time (when the database recorded the fact) and valid-time (when the fact was actually true in the real world).
Why it matters: It allows you to "backdate" corrections. For example, "I learned today (tx-time) that the user actually moved to London last Tuesday (valid-time)."
MesoDB Gap: MesoDB's transact_at allows you to override the timestamp, but the engine effectively treats valid-time and tx-time as the same physical timeline. You have unitemporal time-travel mimicking bitemporality.

The Verdict: Now vs. Later
Here is my candid assessment of what makes the cut for v1 versus what gets pushed to the v0.2 backlog:

Push to v0.2: Excision (Eviction). Modifying immutable Parquet files and rewriting WALs is incredibly complex and dangerous. It requires a dedicated background compaction rewrite process. Don't block v1 for this.

Push to v0.2: Valid-Time Separation. True bitemporality requires doubling the complexity of the DataFusion window queries (valid_from/to AND tx_from/to). Unitemporal history is more than enough for a v1 launch.

Push to v0.2: Transaction Functions. Your atomic CAS implementation is highly capable. You don't need to embed a scripting runtime in the transactor right now.

Do it NOW: Reified Transactions. Because your tx_id is a u64 and your EntityId is a u64, the math aligns perfectly. Being able to tag transactions with a :tx/author or :tx/message makes your new History API infinitely more useful for auditing.
```
