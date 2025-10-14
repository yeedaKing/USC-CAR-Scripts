### export REPO="/Users/dkyee/Desktop/CSCI/USC Digital Phenotype Lab/USC-CAR-Scripts"
### bash "$REPO/pipelines/rapids/scripts/publish_to_processed.sh" "$PID"

#!/usr/bin/env bash
set -euo pipefail
REPO="${REPO:?set REPO}"
PID="${1:?usage: $0 PID}"

SRC="$REPO/pipelines/rapids/data/interim/$PID"
DST="$REPO/pipelines/rapids/data/processed/$PID"
mkdir -p "$DST"

# Doryab variants if present
for f in \
  "$SRC/phone_locations_features/phone_locations_python_doryab_DOR.csv" \
  "$SRC/phone_locations_features/phone_locations_python_doryab_YUN.csv"

do
  if [ -f "$f" ]; then
    base="$(basename "${f/phone_locations_python_/}")"    # doryab_DOR.csv
    out="${base%.csv}_daily.csv"                          # doryab_DOR_daily.csv
    cp "$f" "$DST/$out"
  fi

done

# Barnett daily
[ -f "$SRC/phone_locations_barnett_daily.csv" ] && \
  cp "$SRC/phone_locations_barnett_daily.csv" "$DST/phone_locations_barnett_daily.csv"

echo "Published to $DST"
