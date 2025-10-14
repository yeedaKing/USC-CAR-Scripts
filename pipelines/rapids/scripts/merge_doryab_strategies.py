"""
python "$REPO/pipelines/rapids/scripts/merge_doryab_strategies.py" \
  --dor "$REPO/pipelines/rapids/data/processed/$PID/phone_locations_doryab_DOR_daily.csv" \
  --yun "$REPO/pipelines/rapids/data/processed/$PID/phone_locations_doryab_YUN_daily.csv" \
  --out "$REPO/pipelines/rapids/data/processed/$PID/phone_locations_doryab_MERGED_daily.csv"
"""

#!/usr/bin/env python3
import argparse, pandas as pd
import numpy as np
import pathlib

KEYS = ["local_segment","local_segment_label",
        "local_segment_start_datetime","local_segment_end_datetime"]

def suffix_features(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    df = df.copy()
    # keep keys as-is; suffix only feature columns
    feat_cols = [c for c in df.columns
                 if c not in KEYS]

    rename = {c: f"{c}_{suffix}" for c in feat_cols}
    return df.rename(columns=rename)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dor", required=True, help="phone_locations_doryab_DOR_daily.csv")
    ap.add_argument("--yun", required=True, help="phone_locations_doryab_YUN_daily.csv")
    ap.add_argument("--out", required=True, help="output merged CSV")
    args = ap.parse_args()

    dor = pd.read_csv(args.dor)
    yun = pd.read_csv(args.yun)

    # ensure keys exist
    for k in KEYS:
        if k not in dor.columns or k not in yun.columns:
            raise SystemExit(f"Missing join key '{k}' in one of the inputs.")

    dor_s = suffix_features(dor, "DOR")
    yun_s = suffix_features(yun, "YUN")

    merged = pd.merge(dor_s, yun_s, on=KEYS, how="outer", validate="one_to_one")
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.out, index=False)
    print(f"Wrote {args.out}  (rows: {len(merged)})")

if __name__ == "__main__":
    main()
