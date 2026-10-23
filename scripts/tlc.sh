#!/usr/bin/env bash
# Model-checks spec/tla, then checks that the model is not vacuous: with the
# fencing-token comparison removed from the write guard, TLC must find a
# zombie deciding a job's outcome. Fails if either expectation is not met.
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=${TLA2TOOLS:-$PWD/tla2tools.jar}
SHA=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
if [ ! -f "$JAR" ]; then
  curl -sSfL -o "$JAR" https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
fi
echo "$SHA  $JAR" | shasum -a 256 -c - >/dev/null
tlc() { java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC MCConveyor.tla -config MCConveyor.cfg -workers auto -metadir "$1/.tlc" -cleanup; }

echo "== model"
out=$(cd spec/tla && tlc "$PWD") || { echo "$out"; exit 1; }
echo "$out" | grep -E "No error|states generated|violated"
grep -q "No error has been found" <<<"$out"

echo "== mutant: Holds without the fencing token"
MUT=$(mktemp -d)
trap 'rm -rf "$MUT"' EXIT
cp spec/tla/MCConveyor.tla spec/tla/MCConveyor.cfg "$MUT"/
sed 's/^Holds(w, j) == IsCurrent(w, j) \/\\ state\[j\] = "running"$/Holds(w, j) == token[w][j] # 0 \/\\ state[j] = "running"/' \
  spec/tla/Conveyor.tla > "$MUT/Conveyor.tla"
! cmp -s spec/tla/Conveyor.tla "$MUT/Conveyor.tla" || { echo "mutation did not apply"; exit 1; }
out=$(cd "$MUT" && tlc "$MUT" || true)
echo "$out" | grep -E "No error|states generated|violated"
grep -q "OnlyHolderDecides is violated" <<<"$out"
echo "ok: model passes and the unfenced mutant is caught"
