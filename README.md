# mapdb-java-store

The MapDB5 storage engine: a record store where collections push *operations*
down into the store instead of pulling deserialized arrays up, eliminating
intra-node write amplification. This is the **reference implementation**; there
are separate ports in Rust and Zig.

> **The on-disk format is not stabilised.** There is no compatibility guarantee —
> not between the Java, Rust and Zig implementations, and not between versions of
> any one of them. A file written by one engine may not open under another, or
> under a later build of the same engine. Implementers may change the format
> freely and without notice; there is no migration path and none is planned.
> Do not put data you care about in it.

Requires **JDK 17 or later** and Maven.

```sh
mvn test                          # the unit suite (*Test.java)
mvn -P integration-tests verify   # adds the long-running *IT stress/scale suites
```

`*IT.java` tests are stress, scale and volume suites that take tens of minutes
and several GB of temporary files. They are deliberately **not** part of
`mvn test` or a plain `mvn verify`; the `integration-tests` profile is the only
thing that runs them.

## Quick start

### High-level: `DBMaker` / `DB`

The `org.mapdb.db` facade (ported from MapDB 3) picks a store, keeps a small
on-store *name catalog*, and hands out named collections that reopen themselves —
no manual recid bookkeeping:

```java
import org.mapdb.db.*;
import org.mapdb.ser.*;

DB db = DBMaker.fileDB("data.db").transactionEnable().make(); // crash-safe (StoreWAL)
var map = db.treeMap("myMap", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
            .createOrOpen();
map.put(1L, "one");
db.commit();
db.close();
// reopening the same file + name returns the data:
DB db2 = DBMaker.fileDB("data.db").transactionEnable().make();
var map2 = db2.treeMap("myMap", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open();
```

Factories include heap, direct/byte-array memory, append-only memory, file, and
temporary-file DBs. Named collections include:

- `hashMap` / `hashSet`: plain HTree, expiring/size-bounded cache, external
  values, counters, loaders, overflow, listeners, explicit seeds, and managed
  background expiration;
- `hashMap48` / `hashSet48`: wide-hash HTree variants;
- `treeMap` / `treeSet`: BTree counters, listeners, streaming bulk build, and
  values stored outside nodes;
- `bufferTreeMap` and immutable `sortedTableMap`;
- persistent FIFO queues, LIFO stacks, circular queues, `QueueLong`, index-tree
  lists/maps, and atomic long/integer/boolean/string/var values.

Every DB maker has `create()` / `open()` / `createOrOpen()`. The catalog stores
built-in and recursively parameterized serializer/format descriptors. The DB
also supports read-only wrappers, catalog inspection/verification, rename,
delete, compact, weak/strong shutdown hooks, and transactional rollback.

The catalog lives at **recid 1**; a DB-owned store is exclusively owned.
Mutations are durable only after `db.commit()`; `rollback()` requires
`transactionEnable()`. See [PORTING.md](PORTING.md) for the MapDB 1/2/3 parity
matrix and intentional architectural differences.

### Low-level: `Store` + `GroupFormat`

The low-level API is the primary one for advanced use. Using a path that
does not yet exist, the following creates a BTreeMap in a new durable
StoreDirect file:

```java
import java.io.File;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreDirect;

StoreDirect store = new StoreDirect(new File("data.db"));
BTreeMap<Long, Long> map = BTreeMap.create(
    store, LongFormat.INSTANCE, LongFormat.INSTANCE, 32);
map.put(1L, 100L);
store.commit();

long rootRecidRecid = map.rootRecidRecid();
store.close();
```

Persist `rootRecidRecid` in application metadata. Reopen the store and map with
the same formats and node size:

```java
StoreDirect store = new StoreDirect(new File("data.db"));
BTreeMap<Long, Long> map = BTreeMap.open(
    store, rootRecidRecid, LongFormat.INSTANCE, LongFormat.INSTANCE, 32);
```

For String keys or values, use `StringGroupFormat.INSTANCE` to retain the
binary-searchable serialized representation.

## Layout

| package | contents |
| --- | --- |
| `org.mapdb` | exceptions, `MapExtra`, modification listeners, `Bind`, `QueueLong` |
| `org.mapdb.io` | `DataInput2` (seekable, zero-copy views), `DataOutput2` |
| `org.mapdb.ser` | `Serializer` (element codec), `GroupFormat` (the action-forwarding format: object-side + byte-side ops), `LongFormat` (long[], binary search over serialized bytes), `ObjectArrayFormat` (generic fallback) |
| `org.mapdb.store` | `Store` / `StoreDelta` / `StoreTx` / `RecordRead` interfaces + four implementations |
| `org.mapdb.btree` | `BTreeMap` / `BufferTreeMap` B-link trees, including bottom-up pump bulk build |
| `org.mapdb.db` | `DBMaker` builder + `DB` facade: name catalog (recid 1), collection makers, `Atomic.*`, lifecycle (MapDB 3 port) |
| `org.mapdb.queue` | persistent blocking FIFO, LIFO, and circular queues |

## Stores

- **`StoreOnHeap`** — records are live objects, no serialization; `read()`
  dispatches `onObject`.
- **`StoreByteArray`** — one `byte[]` per record with explicit capacity;
  the *reference* `StoreDelta` implementation and fuzz oracle.
- **`StoreDirect`** — standalone durable paged volume (1 MB slices, heap/direct
  or mmap file), on-volume recid index pages, persistent long-stack free lists,
  crash-detecting header checksum, 16-aligned data allocation, and linked
  chunk chains for records above the plain ~1 MiB capacity.
- **`StoreWAL`** — `StoreTx` + `StoreDelta`: stages mutations in memory, commit
  writes a WAL section (appends logged as deltas — `T_APPEND`) and fsyncs
  before applying to the inner StoreDirect.

  The log is a **segment set, not a file** (on-disk format v3): segments named
  `<db>.wal.<16 hex digits of segmentSeq>`, each with a checksummed header
  carrying its sequence number and first LSN, and length-prefixed sections
  carrying separate header and body CRCs. Recovery validates a CRC *before*
  decoding any entry, applies entry-by-entry in O(1) memory, and distinguishes a
  torn tail (bad section at EOF → truncated) from mid-log bit rot (bad section
  followed by a valid one → `DataCorruption`, never silent loss). LSNs are
  exactly consecutive across the retained set. Reclamation is an unlink of a
  cleaned segment rather than a rewrite of one file, and a budgeted incremental
  cleaner replaces what used to be a whole-store checkpoint pause.

  The two 64-bit values v3 added — a segment's `firstLsn` and the clean mark's
  `logStartLsn` — are what let recovery *check* where the retained log begins
  instead of inferring it. See `StoreWAL`'s class javadoc for the full grammar;
  it is the authority, and the format is not stabilised (see the notice above).

## Key contracts

- Record content = base bytes ++ appended deltas; the store never parses it.
- `append` returns `REFUSED` (-1) when capacity is exhausted — the caller splits.
- `get` of a preallocated recid returns null; N/D states throw `GetVoid`.
- `getAllRecids` excludes preallocated records everywhere.
- Bulk build = `preallocate` → write-once → `update`-fill (v3 Pump pattern).
  `BTreeMap.createFromSorted` and `BufferTreeMap.createFromSorted` build
  bottom-up from strictly ascending entries, with one content write per node.

## Locking

Segment R/W locks (recid low bits) + one structural allocator lock; order is
segment → structural, deadlock-free by hierarchy. Locks are single-entry
`StampedLock` views — store locks are never held
reentrantly, so ReentrantReadWriteLock's per-thread read-hold bookkeeping is
skipped. `DeadlockAsserts` (active under `-ea`) catches A3 callback reentry
(serializer/action calling back into the store), lock-order inversion, and
structural-lock reentrancy. All stores also take a `threadSafe=false`
constructor flag replacing every lock with a shared no-op (asserts stay on).

## Scale (stress suite, `src/test/.../stress/*IT.java`, run manually)

Measured on 32 cores / 128 GB (see `*IT` class comments for exact setups):
200M×16B puts at ~19.6M ops/s; 41.9 GB of ~1 MiB records with free-space
reuse; 100M appends at ~10.3M ops/s; BTree 200M sequential inserts at ~1.18M/s,
full iteration at ~98M entries/s; 64-thread mixed store load ~5.6M ops/s with
zero torn reads (CRC-validated); WAL replay at ~1.46 GB/s. `WalHugeReopenIT`:
a 2.2 GiB WAL reopens via streaming replay at ~1.4 GB/s, checkpoints into a
single >2 GiB snapshot section in ~1 s, and reopens from it at ~1.9 GB/s.

## Known limitations

- StoreWAL transactions stage in memory, so a single commit remains bounded by
  heap even though replay/checkpoint streams large files.
- BTreeMap supports concurrent writers through per-recid node locks while
  preserving lock-free readers. BufferTreeMap uses one map-global writer lock,
  so its writes are thread-safe but serialized.
- HTreeCache currently has one persisted TTL policy: write expiry or access
  expiry. MapDB 3's three independent create/update/get clocks are not yet
  represented.
- BTree external-value maps support normal and navigable operations, but not the
  streaming bulk builder.
- Runtime callbacks, bindings, background executors, and blocking-queue
  conditions must be reattached after reopen and coordinate within one live DB
  handle.
- Sharded hash-map/hash-set makers (mapdb3's `memoryShardedHashMap` /
  `heapShardedHashMap` family, which stripe a map across multiple stores for
  write scaling) are not ported.
- There is no inter-process file lock: mapdb5 does not stop a second OS process
  from opening the same file. Enforcing a single writer is the caller's
  responsibility. See [PORTING.md](PORTING.md) for the full list of unported
  config knobs.

## Status / next steps

The engine, the collection engines, and the high-level MapDB compatibility
facade are operational and covered by the Maven test suite. Legacy-version
compatibility status is tracked in [PORTING.md](PORTING.md).

**This has never been released.** There is no published artifact, the API has
no stability guarantee, and it has not been run in production. Treat the test
suite, not this README, as the statement of what works.

## License

Dual EPL-1.0 / EDL-1.0 (`SPDX-License-Identifier: EPL-1.0 OR BSD-3-Clause`).
See [`LICENSE-EPL-1.0.txt`](LICENSE-EPL-1.0.txt),
[`LICENSE-EDL-1.0.txt`](LICENSE-EDL-1.0.txt) and [`NOTICE.md`](NOTICE.md).
