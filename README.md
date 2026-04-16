# rocksdb-statistics-leak

Reproduces the native (C-heap) memory leak caused by incorrect `Statistics` object lifecycle in the RocksDB Java API. Discovered in Apache Spark's RocksDB state store. Includes jemalloc heap profiling scripts for before/after analysis.

## The Bug

When using the RocksDB Java API, each `Statistics` object allocates a
`CoreLocalArray<StatisticsData>` via `posix_memalign` — one cache-line-aligned slot per
CPU core. On an 8-core machine this is ~560 kB per instance; on a 64-core machine ~4–8 MB.

The Java handle does not point to `StatisticsImpl` directly — it points to a heap-allocated
`shared_ptr<Statistics>`. The native memory is only freed when **all** `shared_ptr` copies
are destroyed (refcount → 0).

### Buggy pattern (original Spark code)

```java
options.setStatistics(new Statistics());   // anonymous — shared_ptr refcount = 2
Statistics nativeStats = options.statistics(); // getter allocates a NEW shared_ptr → refcount = 3

// close():
options.statistics().close();  // creates a temp shared_ptr and destroys it — net change = 0 (NO-OP)
options.close();               // drops Options' copy → refcount = 2
// nativeStats goes out of scope without being closed.
// ~StatisticsImpl() only runs when the JVM GC collects nativeStats AND runs its finalizer.
// Under load this never happens fast enough → C-heap grows without bound.
```

### Fixed pattern

```java
Statistics nativeStats = new Statistics();   // shared_ptr refcount = 1
options.setStatistics(nativeStats);          // Options copies it → refcount = 2

// close():
nativeStats.close();   // drops Java-side shared_ptr → refcount = 1
options.close();       // drops Options' copy → refcount = 0 → ~StatisticsImpl() called immediately
```

## Requirements

- Java 11+
- Maven 3.x
- Linux (reads `/proc/self/status` for RSS reporting)
- [Optional] `libjemalloc` for heap profiling (`apt-get install libjemalloc-dev` / `yum install jemalloc`)
- [Optional] `jeprof` for heap dump analysis (included with jemalloc)

## Build

```bash
mvn clean package -q
```

This produces `target/rocksdb-leak-reproducer-1.0-SNAPSHOT.jar` (fat JAR, no classpath setup needed).

## Run

### Without jemalloc (RSS only)

```bash
# Leak mode — RSS grows ~560 kB per iteration (8-core), linearly
java -jar target/rocksdb-leak-reproducer-1.0-SNAPSHOT.jar --mode leak --iterations 80

# Fixed mode — RSS stays flat after JVM warmup
java -jar target/rocksdb-leak-reproducer-1.0-SNAPSHOT.jar --mode fixed --iterations 80
```

### With jemalloc heap profiling (recommended)

The included scripts auto-detect `libjemalloc.so` and configure profiling automatically:

```bash
# Leak mode — heap dumps written to /tmp/jeprof_leak/
bash run_leak.sh --iterations 80 --delay-ms 200

# Fixed mode — heap dumps written to /tmp/jeprof_fixed/
bash run_fixed.sh --iterations 80 --delay-ms 200
```

### CLI options

| Option | Default | Description |
|---|---|---|
| `--mode leak\|fixed` | `leak` | Which pattern to use |
| `--iterations N` | `60` | Number of RocksDB open/close cycles |
| `--delay-ms N` | `200` | Sleep between iterations (ms) |
| `--no-gc` | off | Suppress periodic `System.gc()` calls |

## Analysing heap dumps

```bash
# After running run_leak.sh / run_fixed.sh:
jeprof --show_bytes --text $(which java) /tmp/jeprof_leak/heap.*.heap
```

In leak mode the top entry will be:

```
rocksdb::port::cacheline_aligned_alloc
  rocksdb::CoreLocalArray<StatisticsImpl::StatisticsData>::CoreLocalArray
    rocksdb::StatisticsImpl::StatisticsImpl
      rocksdb::StatisticsJni::StatisticsJni
        Java_org_rocksdb_Statistics_newStatistics    ← new Statistics() in Java
```

This entry is **absent** in the fixed mode dump.

## Example results (8-core container, 80 iterations)

| Mode | RSS start | RSS end | Growth |
|---|---|---|---|
| **leak** | 62,484 kB | 110,180 kB | **+44.8 MB** (~560 kB/iter, linear) |
| **fixed** | 64,244 kB | 70,344 kB | **+6.0 MB** (JVM warmup only, plateaus) |

Live bytes in jemalloc snapshot:

| Mode | Live allocations | `CoreLocalArray<StatisticsData>` present |
|---|---|---|
| **leak** | 16.8 MB / 75 allocs | **Yes** |
| **fixed** | 3.0 MB / 21 allocs | **No** |
