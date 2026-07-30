# MapDB 1/2/3 → mapdb-java-store porting status

mapdb-java-store is a new store architecture, not an on-disk-format upgrade of
MapDB 1–3. Old database files are not opened directly; copy data through the
public collection APIs. API names are retained where they remain meaningful,
and packages still live under `org.mapdb`.

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
- **Index collapse switch:** directory removal currently always uses this
  engine's collapse behavior; `removeCollapsesIndexTreeDisable()` is not exposed.
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
  independent stores for write scaling, are not ported. This engine exposes
  only the single-store HTreeMap variants.
- **File locking:** the guard itself IS here — a file-backed store holds a
  `FileLock` on `<db>.lock` for as long as it is open, so a second opener of the
  same pathname is refused with a `DBException`. It is exclusive except for a
  read-only `StoreWAL` open, which takes a shared lock (several readers, no
  writer) and none at all on a medium where no writer could exist. What is not
  ported are the mapdb3 knobs around it: `fileLockDisable` (there is no way to
  turn the lock off) and `fileLockWait` (the lock is always `tryLock`, so a
  refusal is immediate and never a timed wait).
- **Unported config knobs:** several DBMaker/StoreDirect switches have no
  equivalent here: `concurrencyScale` (segment count), `fileSyncDisable`,
  `allocateStartSize`/`allocateIncrement`, `volumeDB`, `checksumHeaderBypass`,
  and `fileMmapPreclearDisable`.

These omissions are architectural projects or obsolete implementation details,
not unconnected DB facade switches. The normal Maven suite covers the ported
surface; stress/scale tests remain opt-in integration tests.
