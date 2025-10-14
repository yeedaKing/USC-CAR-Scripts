"""
export REPO="/Users/dkyee/Desktop/CSCI/USC Digital Phenotype Lab/USC-CAR-Scripts"
export PID="c8524dee-8a13-45b9-85f8-9073f7a9177e"

python "$REPO/pipelines/rapids/scripts/summarize_weekly.py" \
  --pid "$PID" \
  --barnett "$REPO/pipelines/rapids/data/interim/$PID/phone_locations_barnett_daily.csv" \
  --doryab  "$REPO/pipelines/rapids/data/interim/$PID/phone_locations_features/phone_locations_python_doryab.csv" \
  --outdir  "$REPO/pipelines/rapids/data/processed/$PID" \
  --week_anchor MON
"""

#!/usr/bin/env python3
import argparse, pandas as pd, numpy as np, pathlib

DORYAB_START = "local_segment_start_datetime"
DORYAB_END   = "local_segment_end_datetime"

DATE_CANDIDATES = ["local_date", "date", "day"]
DATETIME_CANDIDATES = ["local_date_time", "datetime", "timestamp", "start"]

def _to_naive(dt: pd.Series) -> pd.Series:
    """Return a tz-naive datetime Series regardless of input tz-awareness."""
    # pandas: tz-aware → convert to UTC → drop tz
    if getattr(dt.dt, "tz", None) is not None:
        return dt.dt.tz_convert("UTC").dt.tz_localize(None)
        
    # already naive
    return dt

def _pick_datetime(df: pd.DataFrame) -> pd.Series:
    """
    Return a tz-naive pandas datetime Series to anchor the week.
    Prefer Doryab's segment start column if present; otherwise try common
    date/datetime columns.
    """
    # Doryab segment-based
    if DORYAB_START in df.columns:
        s = pd.to_datetime(df[DORYAB_START], errors="coerce")
        if s.notna().any():
            return _to_naive(s)

    # Date-only columns (normalize to midnight)
    for c in DATE_CANDIDATES:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            if s.notna().any():
                return _to_naive(s)

    # Other datetime candidates
    for c in DATETIME_CANDIDATES:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            if s.notna().any():
                return _to_naive(s)

    raise ValueError(
        "Could not infer a datetime anchor. "
        f"Expected '{DORYAB_START}' for Doryab, or one of {DATE_CANDIDATES+DATETIME_CANDIDATES}."
    )

def _week_bounds(dt: pd.Series, week_anchor: str):
    """Compute [week_start, week_end] given a datetime Series and week anchor."""
    week_anchor = week_anchor.upper()
    if week_anchor.startswith("MON"):
        week_start = dt - pd.to_timedelta(dt.dt.weekday, unit="D")     # Mon–Sun

    elif week_anchor.startswith("SUN"):
        week_start = dt - pd.to_timedelta((dt.dt.weekday + 1) % 7, unit="D")  # Sun–Sat

    else:
        raise ValueError("week_anchor must be 'MON' or 'SUN'.")

    week_start = week_start.dt.normalize()
    week_end = week_start + pd.Timedelta(days=6)
    return week_start, week_end

def weekly_agg(df: pd.DataFrame, provider_name: str, pid: str, week_anchor: str="MON") -> pd.DataFrame:
    dt = _pick_datetime(df)
    week_start, week_end = _week_bounds(dt, week_anchor)

    df = df.copy()
    df["week_start"] = week_start
    df["week_end"]   = week_end

    # choose numeric feature columns; ignore obvious non-features
    ignore = {
        "week_start","week_end",
        "local_date","date","day",
        "local_date_time","datetime","timestamp","start","end","label",
        DORYAB_START, DORYAB_END,
        "local_segment","local_segment_label"
    }
    num_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c not in ignore]
    if not num_cols:
        raise ValueError(f"No numeric columns found to aggregate for {provider_name}.")

    grp = df.groupby(["week_start", "week_end"], as_index=False)
    out_sum  = grp[num_cols].sum().add_suffix("_sum")
    out_mean = grp[num_cols].mean().add_suffix("_mean")
    base = grp.size().rename(columns={"size":"n_rows"})

    weekly = base.join([out_sum, out_mean])
    weekly.insert(0, "pid", pid)
    weekly.insert(1, "provider", provider_name)
    return weekly

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", required=True)
    ap.add_argument("--barnett", required=True, help="Path to phone_locations_barnett_daily.csv")
    ap.add_argument("--doryab", required=True, help="Path to phone_locations_python_doryab.csv")
    ap.add_argument("--outdir", required=True, help="Output directory for weekly CSVs")
    ap.add_argument("--week_anchor", default="MON", choices=["MON","SUN"])
    args = ap.parse_args()

    pid = args.pid
    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Barnett weekly
    bdf = pd.read_csv(args.barnett)
    b_weekly = weekly_agg(bdf, "barnett", pid, args.week_anchor)
    b_out = outdir / "phone_locations_barnett_weekly.csv"
    b_weekly.to_csv(b_out, index=False)

    # Doryab weekly
    ddf = pd.read_csv(args.doryab)
    d_weekly = weekly_agg(ddf, "doryab", pid, args.week_anchor)
    d_out = outdir / "phone_locations_doryab_weekly.csv"
    d_weekly.to_csv(d_out, index=False)

    print(f"Wrote:\n  {b_out}\n  {d_out}")

if __name__ == "__main__":
    main()
