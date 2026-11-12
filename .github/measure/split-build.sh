#!/bin/bash
# Temporary (census #153): run inside the census's `env` stage, under
# `pixi run`, by .github/workflows/measure-split-build.yml. Builds the
# programs of multicall-pipelines four ways, each with an empty Mojo compile
# cache of its own, and writes the results to $RESULTS:
#
#   combined  multicall-pipelines, as scripts/multicall.sh builds it
#   seq       each program alone, one after the other
#   par4      each program alone, 4 at a time (xargs -P 4)
#   par8      each program alone, 8 at a time
#
# The flags are scripts/build.sh's in the image: -g1 --target-cpu x86-64-v3
# and the two -D MORCA_* values.
set -eu
RESULTS=${RESULTS:-/results}
CENSUS_SHA=${CENSUS_SHA:-unknown}
mkdir -p "$RESULTS"
trap 'chown -R "${HOST_UID:-0}:${HOST_GID:-0}" "$RESULTS" 2> /dev/null || true' EXIT
cd /src/project
flags=(-g1 --target-cpu x86-64-v3 -D MORCA_SOURCE=https://github.com/morca-dev/census -D "MORCA_COMMIT=$CENSUS_SHA")
inc=(-I bin -I pipelines -I src)

# The programs of multicall-pipelines, as scripts/build.sh lists them.
apps="bin/scheduler.mojo"
[ -f bin/console.mojo ] && apps="$apps bin/console.mojo"
progs=""
for f in bin/*.mojo pipelines/*.mojo; do
    [ -f "$f" ] || continue
    case " $apps " in *" $f "*) continue ;; esac
    progs="$progs $f"
done
echo "programs:$progs"
echo "nproc $(nproc), $(grep -m1 'model name' /proc/cpuinfo), mem $(awk '/^MemTotal/ {printf "%d", $2 / 1048576}' /proc/meminfo) GB"
mojo --version

# The compile cache modular.cfg names, emptied before each measurement, and
# MODULAR_CACHE_DIR, a new empty directory for each mojo build.
cfg_cache=$(sed -n 's/^cache_dir *= *//p' "$CONDA_PREFIX/share/max/modular.cfg" | head -1)
echo "modular.cfg cache_dir: ${cfg_cache:-none}"
cold() {
    if [ -n "$cfg_cache" ]; then rm -rf "$cfg_cache"; fi
    rm -rf /caches
    mkdir -p /caches
}

# Peak anonymous memory of the container's cgroup (what the builds hold,
# not the page cache), sampled every 0.5 s while a measurement runs.
sampler_start() { # FILE
    ( peak=0
      while :; do
          a=$(awk '/^anon / {print $2}' /sys/fs/cgroup/memory.stat 2> /dev/null || echo 0)
          [ "$a" -gt "$peak" ] && { peak=$a; echo "$peak" > "$1"; }
          sleep 0.5
      done ) &
    sampler=$!
}
sampler_stop() { kill "$sampler" 2> /dev/null || true; wait "$sampler" 2> /dev/null || true; }

# GNU time's -v fields.
tv() { sed -n "s/^[[:space:]]*$2: //p" "$1" | head -1; }

# One program alone: OUT_DIR PROGRAM.mojo, timed into OUT_DIR/NAME.time.
cat > /usr/local/bin/build-one << 'EOF'
#!/bin/bash
set -eu
out=$1 prog=$2
name=$(basename "$prog" .mojo)
export MODULAR_CACHE_DIR=/caches/$name-$$
mkdir -p "$MODULAR_CACHE_DIR"
s=$(date +%s.%N)
/usr/bin/time -v -o "$out/$name.time" mojo build $INC $FLAGS "$prog" -o "$out/$name" > "$out/$name.log" 2>&1 \
    || { echo "FAILED $name"; cat "$out/$name.log"; exit 1; }
echo "$(date +%s.%N) $s" | awk '{printf "%.1f\n", $1 - $2}' > "$out/$name.wall"
echo "  $name $(cat "$out/$name.wall") s"
EOF
chmod +x /usr/local/bin/build-one
export INC="${inc[*]}" FLAGS="${flags[*]}"

load() { cut -d' ' -f1-3 /proc/loadavg; }
# MEASUREMENT: wall, user, sys, max RSS of one process, peak anon, load.
record() { # NAME TIMEFILE WALL PEAKFILE LOAD0
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s → %s\n' "$1" "$3" "$(tv "$2" 'User time (seconds)')" \
        "$(tv "$2" 'System time (seconds)')" "$(tv "$2" 'Maximum resident set size (kbytes)')" \
        "$(cat "$4" 2> /dev/null || echo 0)" "$5" "$(load)" >> "$RESULTS/runs.tsv"
}
: > "$RESULTS/runs.tsv"
: > "$RESULTS/seq.tsv"

# 1. combined
echo "== combined"
cold
l0=$(load)
sampler_start /tmp/peak-combined
s=$(date +%s)
MODULAR_CACHE_DIR=/caches/combined /usr/bin/time -v -o /tmp/combined.time \
    sh scripts/multicall.sh multicall-pipelines /m/combined/bin /m/combined/pipelines $progs -- "${flags[@]}"
w=$(($(date +%s) - s))
sampler_stop
record combined /tmp/combined.time "$w" /tmp/peak-combined "$l0"
echo "  $w s; cache $(du -sh /caches | cut -f1)"

# 2. each alone, one after the other
echo "== seq"
cold
mkdir -p /m/seq
l0=$(load)
sampler_start /tmp/peak-seq
s=$(date +%s)
for p in $progs; do
    build-one /m/seq "$p"
done
w=$(($(date +%s) - s))
sampler_stop
for p in $progs; do
    n=$(basename "$p" .mojo)
    printf '%s\t%s\t%s\t%s\t%s\n' "$n" "$(cat /m/seq/$n.wall)" "$(tv /m/seq/$n.time 'User time (seconds)')" \
        "$(tv /m/seq/$n.time 'System time (seconds)')" "$(tv /m/seq/$n.time 'Maximum resident set size (kbytes)')" \
        >> "$RESULTS/seq.tsv"
done
# The sequence as one measurement: its user and sys are the programs' sums.
awk -F'\t' -v w="$w" '{u += $3; y += $4; if ($5 > m) m = $5}
    END {printf "\tUser time (seconds): %.2f\n\tSystem time (seconds): %.2f\n\tMaximum resident set size (kbytes): %d\n", u, y, m}' \
    "$RESULTS/seq.tsv" > /tmp/seq.time
record seq /tmp/seq.time "$w" /tmp/peak-seq "$l0"
echo "  $w s; cache $(du -sh /caches | cut -f1)"

# 3. each alone, N at a time
for n in 4 8; do
    echo "== par$n"
    cold
    mkdir -p /m/par$n
    l0=$(load)
    sampler_start /tmp/peak-par$n
    s=$(date +%s)
    printf '%s\n' $progs | /usr/bin/time -v -o /tmp/par$n.time xargs -P "$n" -n 1 build-one /m/par$n
    w=$(($(date +%s) - s))
    sampler_stop
    record par$n /tmp/par$n.time "$w" /tmp/peak-par$n "$l0"
    echo "  $w s; cache $(du -sh /caches | cut -f1)"
done
# xargs's own time sums its children's user and sys; max RSS is the
# largest one program's.

# 4. sizes: raw, then with the Dockerfile's objcopy, then gzipped.
echo "== sizes"
size_of() { # FILES... -> raw objcopied gzipped (sums)
    r=0 c=0 z=0
    for f in "$@"; do
        r=$((r + $(stat -c %s "$f")))
        cp "$f" /tmp/sz && objcopy --compress-debug-sections=zlib /tmp/sz
        c=$((c + $(stat -c %s /tmp/sz)))
        z=$((z + $(gzip -c /tmp/sz | wc -c)))
    done
    echo "$r $c $z"
}
sep=""
for p in $progs; do sep="$sep /m/seq/$(basename "$p" .mojo)"; done
echo "combined $(size_of /m/combined/bin/multicall-pipelines)" > "$RESULTS/sizes.txt"
echo "separate $(size_of $sep)" >> "$RESULTS/sizes.txt"
for p in $progs; do
    n=$(basename "$p" .mojo)
    echo "$n $(size_of /m/seq/$n)" >> "$RESULTS/sizes-each.txt"
done
cat "$RESULTS/sizes.txt"
cat /sys/fs/cgroup/memory.peak > "$RESULTS/memory.peak" 2> /dev/null || true

# The summary.
mb() { awk -v k="$1" 'BEGIN {printf "%.1f", k / 1048576}'; }
{
    echo "### Census #153: multicall-pipelines vs its programs built alone"
    echo
    echo "census $CENSUS_SHA, $(nproc) threads ($(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')), $(mojo --version | head -1). Flags: \`${flags[*]}\`. Each measurement starts with an empty Mojo compile cache (MODULAR_CACHE_DIR new for each mojo build)."
    echo
    echo "| build | wall s | user+sys s | CPU / wall | largest process RSS GB | peak memory (cgroup anon) GB | load before → after |"
    echo "|---|---|---|---|---|---|---|"
    while IFS=$'\t' read -r name w u y rss peak ld; do
        awk -v n="$name" -v w="$w" -v u="$u" -v y="$y" -v r="$rss" -v p="$peak" -v l="$ld" \
            'BEGIN {printf "| %s | %d | %.0f | %.1f | %.1f | %.1f | %s |\n", n, w, u + y, (u + y) / w, r / 1048576, p / 1073741824, l}'
    done < "$RESULTS/runs.tsv"
    echo
    echo "Each program alone (seq):"
    echo
    echo "| program | wall s | user+sys s | max RSS GB |"
    echo "|---|---|---|---|"
    awk -F'\t' '{printf "| %s | %.0f | %.0f | %.1f |\n", $1, $2, $3 + $4, $5 / 1048576; w += $2; c += $3 + $4}
        END {printf "| **sum** | %.0f | %.0f | |\n", w, c}' "$RESULTS/seq.tsv"
    echo
    echo "Sizes (MB; objcopied = with the Dockerfile's \`objcopy --compress-debug-sections=zlib\`, gzip = \`gzip -c | wc -c\` of that):"
    echo
    echo "| | raw | objcopied | gzip |"
    echo "|---|---|---|---|"
    while read -r name r c z; do
        awk -v n="$name" -v r="$r" -v c="$c" -v z="$z" 'BEGIN {printf "| %s | %.1f | %.1f | %.1f |\n", n, r / 1e6, c / 1e6, z / 1e6}'
    done < "$RESULTS/sizes.txt"
    echo
    echo "<details><summary>each separate binary</summary>"
    echo
    echo "| program | raw | objcopied | gzip |"
    echo "|---|---|---|---|"
    while read -r name r c z; do
        awk -v n="$name" -v r="$r" -v c="$c" -v z="$z" 'BEGIN {printf "| %s | %.1f | %.1f | %.1f |\n", n, r / 1e6, c / 1e6, z / 1e6}'
    done < "$RESULTS/sizes-each.txt"
    echo
    echo "</details>"
} > "$RESULTS/summary.md"
cat "$RESULTS/summary.md"
