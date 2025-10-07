import pandas as pd

INPUT_FILE = "data/tweets_raw.csv"
OUTPUT_FILE = "data/gps_points.csv"

def main():
    df = pd.read_csv(INPUT_FILE)

    gps_df = df[["longitude", "latitude"]].copy().dropna(subset=["latitude","longitude"])
    gps_df["row_id"] = range(1, len(gps_df)+1)
    gps_df["latlon"] = gps_df["latitude"].round(6).astype(str) + "," + gps_df["longitude"].round(6).astype(str)

    gps_df.to_csv(OUTPUT_FILE, index=False, columns=["row_id", "longitude", "latitude", "latlon"])

if __name__ == "__main__":
    main()