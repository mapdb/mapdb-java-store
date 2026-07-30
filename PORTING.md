# MapDB 1/2/3 → mapdb5 porting status

Mapdb5 is a Store4 implementation, not an on-disk-format upgrade of MapDB 1–3.
Old database files are not opened directly; copy data through the public
collection APIs. API names are retained where they remain meaningful, but
mapdb5 packages live under `org.mapdb`.

## Ported

- DB/DBMaker name catalog, typed makers, create/open/create-or-open, generic
  dispatch, catalog inspection and verification, rename/delete, commit,
  rollback, compact, read-only mode, temp files, delete-after-open/close, and
  shutdown hooks.
- HTree maps/sets: segmented layouts, explicit hash seeds and hashers, external
  values, 48-bit hash variant, persistent counters, modification listeners,
  value loaders, write/access expiry, max-size/store-size bounds, overflow,
  eviction callbacks, foreground throttling, and caller-executor background
  sweeps.
- BTree maps/sets: full concurrent navigable views, counters, modification
  listeners, sorted pump/sink builds, and external-value records.
- BufferTreeMap and SortedTableMap DB makers and bulk builders.
- Atomic long/integer/boolean/string/var; numeric atomics implement `Number`.
- IndexTreeList, IndexTreeLongLongMap, QueueLong, persistent blocking FIFO,
  LIFO stack, and overwrite-on-full circular queue.
- Runtime collection helpers: `MapModificationListener`, `ModificationAwareMap`,
  `MapExtra`, and `Bind` secondary values/keys, inverse maps, histograms, delete
  sinks, and size bindings.
- Built-in primitive, array, string, packed, recid, UUID, date, class, big-number,
  Java-object, array-wrapper, compression, delta/prefix, tuple, object-array, and
  columnar serializers/formats. Parameterized codecs have recursive catalog
  descriptors.
- Modern equivalents of heap, byte-array, direct-memory, mmap file, WAL, and
  append-only stores, plus incremental/full compaction and maintenance tasks.

## Deliberate differences or remaining deep work

- **Expiry clocks:** HTreeCache persists one TTL policy, either writes
  (create/update) or accesses. MapDB 3 allowed independent create, update, and
  get queues. Combining different clocks requires a new multi-queue entry format.
- **Index collapse switch:** directory removal currently always uses mapdb5's
  collapse behavior; `removeCollapsesIndexTreeDisable()` is not exposed.
- **External BTree pump:** external-value trees support live operations and
  reopen, but `createFrom`/streaming sink currently reject that mode.
- **Transactions/snapshots:** StoreWAL supplies commit/rollback and crash
  recovery. The old Engine-based `TxMaker`, `TxBlock`, `TxEngine`, and live
  snapshot API are not recreated.
- **Legacy Engine wrappers:** AsyncWriteEngine, StoreCached, the old reference
  cache strategies, metrics executors, and record-condition caches belonged to
  the removed Engine layer. Store4 collections operate directly on Store.
- **Legacy storage backends:** StoreArchive, RAF/FileChannel variants, the old
  Volume class hierarchy, and unsafe-specific implementations are superseded by
  StoreDirect/ByteBufferVol rather than copied class-for-class.
- **Encryption:** the obsolete XTEA volume wrapper is not ported. Encryption
  should be supplied below the store (encrypted filesystem/block device) or by a
  modern authenticated-encryption volume.
- **POJO graph serialization:** Java serialization is available, but the old
  SerializerPojo/Elsa class-registration graph serializer is not reproduced.
- **Low-level utility collections:** old internal LongHashMap/LRU/cache helper
  classes are implementation details and were not copied into the public API.
- **Blocking coordination:** persistent queue contents are durable, while Java
  `Condition` wakeups coordinate threads sharing the same live queue handle; they
  are not cross-process signals.
- **Sharded hash makers:** the mapdb3 `memoryShardedHashMap`/`heapShardedHashMap`
  (and the corresponding set) factories, which stripe a map across several
  independent stores for write scaling, are not ported. mapdb5 exposes only the
  single-store HTreeMap variants.
- **File locking:** the mapdb3 `fileLockDisable`/`fileLockWait` knobs and their
  underlying `FileLock` guard are not ported. mapdb5 has no protection against a
  second OS process opening the same file concurrently — the single-writer
  invariant is the caller's responsibility.
- **Unported config knobs:** several DBMaker/StoreDirect switches have no mapdb5
  equivalent: `concurrencyScale` (segment count), `fileSyncDisable`,
  `allocateStartSize`/`allocateIncrement`, `volumeDB`, `checksumHeaderBypass`,
  and `fileMmapPreclearDisable`.

These omissions are architectural projects or obsolete implementation details,
not unconnected DB facade switches. The normal Maven suite covers the ported
surface; stress/scale tests remain opt-in integration tests.
