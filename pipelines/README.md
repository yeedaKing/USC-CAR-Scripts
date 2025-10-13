PID="c8524dee-8a13-45b9-85f8-9073f7a9177e"
REPO="/Users/dkyee/Desktop/CSCI/USC Digital Phenotype Lab/USC-CAR-Scripts"
IMG="moshiresearch/rapids:latest"

docker run -it --rm \
  -v "$REPO":/work \
  -w /work/pipelines/rapids \
  "$IMG" \
  bash -lc 'snakemake --configfile rapids.yaml -j 4 \
    data/interim/'"$PID"'/phone_locations_features/phone_locations_python_doryab.csv \
    data/interim/'"$PID"'/phone_locations_barnett_daily.csv'
