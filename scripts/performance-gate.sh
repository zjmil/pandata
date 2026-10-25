#!/usr/bin/env bash
set -euo pipefail
# Both workflows and local comparisons use this exact driver.
root=$(cd "$(dirname "$0")/.." && pwd)
base=${1:?Usage: performance-gate.sh BASE HEAD [ARTIFACT_DIR]}
head=${2:?Usage: performance-gate.sh BASE HEAD [ARTIFACT_DIR]}
artifacts=${3:-"$root/performance-results"}
mkdir -p "$artifacts"
artifacts=$(cd "$artifacts" && pwd)
run_gate() {
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
base=$(git -C "$root" rev-parse --verify "$base^{commit}")
head=$(git -C "$root" rev-parse --verify "$head^{commit}")
export POLARS_MAX_THREADS=2
export CARGO_BUILD_JOBS=2
export RUSTUP_TOOLCHAIN=1.99.1
export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
if ! command -v rustup >/dev/null 2>&1; then
    python3 - "$work/rustup-init.sh" <<'PYTHON'
import sys
import urllib.request
urllib.request.urlretrieve("https://sh.rustup.rs", sys.argv[1])
PYTHON
    sh "$work/rustup-init.sh" -y --profile minimal --default-toolchain none --no-modify-path 2>&1 | tee "$artifacts/rustup-install.log"
fi
rustup toolchain install "$RUSTUP_TOOLCHAIN" --profile minimal 2>&1 | tee "$artifacts/toolchain.log"
printf 'base=%s\nhead=%s\nthreads=%s\ntoolchain=%s\n' "$base" "$head" "$POLARS_MAX_THREADS" "$RUSTUP_TOOLCHAIN" > "$artifacts/revisions.txt"
# Export source without altering the user's checkout or executing git hooks.
for revision in base head; do
    mkdir "$work/$revision"
    sha=$base
    if [[ $revision == head ]]; then sha=$head; fi
    git -C "$root" archive "$sha" | tar -x -C "$work/$revision"
done
# Overlay the proposed workload on both revisions, including its fixture.
for revision in base head; do
    mkdir -p "$work/$revision/benches" "$work/$revision/tests/fixtures"
    cp "$work/head/benches/conversions.rs" "$work/$revision/benches/workload.rs"
    if [[ $revision == base ]]; then
        cp "$work/head/tests/fixtures/benchmark-strings.txt" "$work/base/tests/fixtures/benchmark-strings.txt"
    fi
    cat >> "$work/$revision/Cargo.toml" <<'TOML'

[[bench]]
name = "performance_workload"
path = "benches/workload.rs"
harness = false
test = false
required-features = ["csv", "json", "parquet"]
TOML
    target="$work/$revision/target"
    # CI restores the base build from cache so only the head revision compiles.
    if [[ $revision == base && -n ${PERF_BASE_TARGET_DIR:-} ]]; then target=$PERF_BASE_TARGET_DIR; fi
    # A restored target may hold an older workload binary; measure() expects exactly one.
    rm -f "$target/release/deps/"performance_workload-*
    (cd "$work/$revision" && CARGO_TARGET_DIR="$target" cargo build --release --locked --no-default-features --features csv,json,parquet --bench performance_workload) 2>&1 | tee "$artifacts/build-$revision.log"
done
measure() {
    local revision=$1 round=$2
    shift 2
    local binaries=()
    local binary
    local target="$work/$revision/target"
    if [[ $revision == base && -n ${PERF_BASE_TARGET_DIR:-} ]]; then target=$PERF_BASE_TARGET_DIR; fi
    for binary in "$target/release/deps/"performance_workload-*; do
        if [[ -f $binary && -x $binary && ${binary##*/} != *.* ]]; then binaries+=("$binary"); fi
    done
    if [[ ${#binaries[@]} != 1 ]]; then
        echo "Expected exactly one benchmark binary for $revision" >&2
        return 1
    fi
    "${binaries[0]}" --output "$artifacts/$round-$revision.json" "$@" 2>&1 | tee "$artifacts/$round-$revision.log"
}
measure base initial
measure head initial
python3 "$root/scripts/compare-performance.py" "$artifacts/initial-base.json" "$artifacts/initial-head.json" \
    --candidates "$artifacts/candidates.txt" --markdown "$artifacts/summary.md" | tee "$artifacts/comparison.log"
if [[ -s "$artifacts/candidates.txt" ]]; then
    cases=$(cat "$artifacts/candidates.txt")
    measure head confirmation --cases "$cases"
    measure base confirmation --cases "$cases"
    python3 "$root/scripts/compare-performance.py" "$artifacts/initial-base.json" "$artifacts/initial-head.json" \
        --candidates "$artifacts/candidates.txt" --confirmation-base "$artifacts/confirmation-base.json" \
        --confirmation-head "$artifacts/confirmation-head.json" --markdown "$artifacts/summary.md" | tee "$artifacts/confirmation-comparison.log"
fi
}
run_gate 2>&1 | tee "$artifacts/driver.log"
