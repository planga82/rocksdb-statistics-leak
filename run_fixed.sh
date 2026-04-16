#!/usr/bin/env bash
# =============================================================================
# run_fixed.sh — demonstrates the corrected Statistics lifecycle (no leak)
#
# Runs LeakReproducer in FIXED mode (mirrors the corrected Spark code).
# VmRSS should stay flat after the JVM warmup phase.
#
# With jemalloc profiling enabled, heap snapshots are written to /tmp/jeprof/.
# Analyse them with:
#   jeprof --show_bytes --text $(which java) /tmp/jeprof/heap.*.heap
# CoreLocalArray<StatisticsData> should NOT appear in the top entries.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$SCRIPT_DIR/target/rocksdb-leak-reproducer-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Building first..."
  mvn -f "$SCRIPT_DIR/pom.xml" clean package -q
fi

# Detect jemalloc library path (works on Debian/Ubuntu and RHEL/CentOS)
JEMALLOC_PATH=""
for candidate in \
    /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
    /usr/lib/x86_64-linux-gnu/libjemalloc.so \
    /usr/lib/libjemalloc.so.2 \
    /usr/lib/libjemalloc.so \
    /usr/local/lib/libjemalloc.so; do
  if [ -f "$candidate" ]; then
    JEMALLOC_PATH="$candidate"
    break
  fi
done

if [ -z "$JEMALLOC_PATH" ]; then
  echo "[WARN] libjemalloc not found — running without heap profiling."
  echo "       Install with: apt-get install libjemalloc-dev  (or yum install jemalloc)"
  echo ""
  exec java -jar "$JAR" --mode fixed "$@"
fi

echo "Using jemalloc: $JEMALLOC_PATH"
mkdir -p /tmp/jeprof

export MALLOC_CONF="prof:true,lg_prof_interval:30,prof_prefix:/tmp/jeprof/heap"

echo "Heap snapshots will be written to /tmp/jeprof/heap.*.heap"
echo "After the run, analyse with:"
echo "  jeprof --show_bytes --text \$(which java) /tmp/jeprof/heap.*.heap"
echo ""

LD_PRELOAD="$JEMALLOC_PATH" exec java \
  -jar "$JAR" \
  --mode fixed \
  "$@"
