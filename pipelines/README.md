# Env variables:

PID="c8524dee-8a13-45b9-85f8-9073f7a9177e"
REPO="/Users/dkyee/Desktop/CSCI/USC Digital Phenotype Lab/USC-CAR-Scripts"
IMG="moshiresearch/rapids:latest"

# Run rapids:

docker run -it --rm \
  -v "$REPO":/work \
  -w /work/pipelines/rapids \
  "$IMG" \
  bash -lc 'snakemake --configfile rapids.yaml -j 4 \
    data/interim/'"$PID"'/phone_locations_features/phone_locations_python_doryab.csv \
    data/interim/'"$PID"'/phone_locations_barnett_daily.csv'

# Remove intermediate csv files:

rm -f "$REPO/pipelines/rapids/data/interim/$PID/"phone_locations_* \
      "$REPO/pipelines/rapids/data/interim/time_segments/${PID}_time_segments"*.csv 2>/dev/null || true

# Force run:

docker run -it --rm \
  -v "$REPO":/work \
  -w /work/pipelines/rapids \
  "$IMG" \
  bash -lc 'snakemake --configfile rapids.yaml -j 4 \
    -R process_phone_locations_types \
       phone_locations_processed_with_datetime \
       phone_locations_add_doryab_extra_columns \
       resample_episodes \
       resample_episodes_with_datetime \
       phone_locations_python_features \
       phone_locations_barnett_daily_features \
    data/interim/'"$PID"'/phone_locations_features/phone_locations_python_doryab.csv \
    data/interim/'"$PID"'/phone_locations_barnett_daily.csv'