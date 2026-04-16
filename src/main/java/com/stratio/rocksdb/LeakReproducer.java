package com.stratio.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/**
 * Reproduces the native (C-heap) memory leak caused by incorrect Statistics lifecycle
 * management in Spark's RocksDB state store wrapper.
 *
 * USAGE
 * -----
 *   java -jar rocksdb-leak-reproducer-1.0-SNAPSHOT.jar [options]
 *
 *   --mode <leak|fixed>   Which pattern to use (default: leak)
 *   --iterations <N>      Number of RocksDB open/close cycles (default: 60)
 *   --delay-ms <ms>       Sleep between iterations for RSS sampling (default: 200)
 *   --no-gc               Suppress System.gc() calls (default: GC every 10 iters)
 *
 * MONITORING
 * ----------
 * The app prints VmRSS (kB) from /proc/self/status after every iteration.
 */
public class LeakReproducer {

    public static void main(String[] args) throws Exception {
        String mode = "leak";
        int iterations = 60;
        long delayMs = 200;
        boolean forceGc = true;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode":       mode = args[++i]; break;
                case "--iterations": iterations = Integer.parseInt(args[++i]); break;
                case "--delay-ms":   delayMs = Long.parseLong(args[++i]); break;
                case "--no-gc":      forceGc = false; break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.exit(1);
            }
        }

        if (!mode.equals("leak") && !mode.equals("fixed")) {
            System.err.println("--mode must be 'leak' or 'fixed'");
            System.exit(1);
        }

        RocksDB.loadLibrary();

        System.out.printf("=== RocksDB Statistics Leak Reproducer ===%n");
        System.out.printf("Mode       : %s%n", mode);
        System.out.printf("Iterations : %d%n", iterations);
        System.out.printf("Delay ms   : %d%n", delayMs);
        System.out.printf("Force GC   : %b%n", forceGc);
        System.out.printf("JVM        : %s%n", System.getProperty("java.vm.name"));
        System.out.printf("%n");
        System.out.printf("%-6s  %-12s  %-12s  %s%n", "Iter", "VmRSS (kB)", "Delta (kB)", "Note");
        System.out.printf("%s%n", "-".repeat(55));

        long baselineRss = -1;
        long prevRss = -1;

        for (int i = 1; i <= iterations; i++) {
            // Force GC every 10 iterations so we can observe that even with GC
            // the native memory remains pinned in leak mode.
            if (forceGc && i % 10 == 0) {
                System.gc();
                System.runFinalization();
                Thread.sleep(50);
            }

            Path dbDir = Files.createTempDirectory("rocksdb_leak_test_");
            try {
                if (mode.equals("leak")) {
                    runLeakIteration(dbDir.toString());
                } else {
                    runFixedIteration(dbDir.toString());
                }
            } finally {
                deleteDirectory(dbDir);
            }

            if (delayMs > 0) {
                Thread.sleep(delayMs);
            }

            long rss = readVmRssKb();
            if (baselineRss < 0) baselineRss = rss;
            long delta = (prevRss < 0) ? 0 : (rss - prevRss);
            prevRss = rss;

            String note = (forceGc && i % 10 == 0) ? "<-- GC forced" : "";
            System.out.printf("%-6d  %-12d  %-+12d  %s%n", i, rss, delta, note);
        }

        long totalGrowth = prevRss - baselineRss;
        System.out.printf("%n--- Summary ---%n");
        System.out.printf("Mode             : %s%n", mode);
        System.out.printf("RSS at start     : %d kB%n", baselineRss);
        System.out.printf("RSS at end       : %d kB%n", prevRss);
        System.out.printf("Total RSS growth : %+d kB  (~%+.1f MB)%n",
                totalGrowth, totalGrowth / 1024.0);

        if (mode.equals("leak")) {
            System.out.printf("%nExpected: significant RSS growth (~4-8 MB per iteration on 64-core).%n");
            System.out.printf("If growth is small, the JVM finalizer happened to run between iterations.%n");
            System.out.printf("Re-run with --no-gc --delay-ms 0 for more aggressive accumulation.%n");
        } else {
            System.out.printf("%nExpected: flat or minimal RSS growth (only JVM baseline fluctuation).%n");
        }
    }

    // -------------------------------------------------------------------------
    // LEAK pattern — mirrors the original buggy Spark RocksDB.scala code
    // -------------------------------------------------------------------------
    private static void runLeakIteration(String dbPath) throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true);

        options.setStatistics(new Statistics());
        Statistics nativeStats = options.statistics();

        RocksDB db = null;
        try {
            db = RocksDB.open(options, dbPath);
            doWork(db);
        } catch (RocksDBException e) {
            throw e;
        } finally {
            if (db != null) db.close();
        }

        options.statistics().close();
        options.close();
    }

    // -------------------------------------------------------------------------
    // FIXED pattern — mirrors the corrected RocksDB.scala after our fix
    // -------------------------------------------------------------------------
    private static void runFixedIteration(String dbPath) throws RocksDBException {
        Statistics nativeStats = new Statistics();
        Options options = new Options()
                .setCreateIfMissing(true)
                .setStatistics(nativeStats);

        RocksDB db = null;
        try {
            db = RocksDB.open(options, dbPath);
            doWork(db);
        } catch (RocksDBException e) {
            throw e;
        } finally {
            if (db != null) db.close();
        }

        nativeStats.close();
        options.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Put and get a handful of keys to ensure the statistics counters are exercised. */
    private static void doWork(RocksDB db) throws RocksDBException {
        for (int i = 0; i < 20; i++) {
            byte[] key = ("rocksdb-leak-test-key-" + i).getBytes();
            byte[] value = ("value-" + i + "-x".repeat(64)).getBytes();
            db.put(key, value);
            db.get(key);
        }
    }

    /**
     * Reads VmRSS from /proc/self/status (Linux only).
     * Returns value in kB, or -1 if unavailable.
     */
    private static long readVmRssKb() {
        try {
            for (String line : Files.readAllLines(Paths.get("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    // format: "VmRSS:   123456 kB"
                    String[] parts = line.trim().split("\\s+");
                    return Long.parseLong(parts[1]);
                }
            }
        } catch (IOException ignored) {
        }
        return -1;
    }

    /** Recursively delete a temp directory after each iteration. */
    private static void deleteDirectory(Path dir) {
        try {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.delete(p); } catch (IOException ignored) {}
                    });
        } catch (IOException ignored) {
        }
    }
}
