import argparse
import pandas as pd
from datetime import date, datetime

# python3 generate_summary.py --file "data/gps_points_geocoded.csv" --filter

parser = argparse.ArgumentParser()
parser.add_argument("--file", '-f', type=str, required=True, help="Path to reversed geocode csv file")
parser.add_argument("--filter", action=argparse.BooleanOptionalAction, default=False,
                    help="Filter location_type LatLon (use --filter / --no-filter)")

def create_dict():
    locations = {
        "St. Louis": {},
        "Carondelet": {},
        "Central West End": {},
        "Cherokee Antique Row": {},
        "Cherokee Street": {},
        "Chesterfield": {},
        "Clayton": {},
        "Downtown St. Louis": {},
        "Eureka": {},
        "Forest Park": {},
        "Grand Center Arts District": {},
        "Kimmswick": {},
        "Kirkwood": {},
        "Laclede's Landing": {},
        "Lafayette Square": {},
        "Maplewood": {},
        "Maryland Heights": {},
        "North County": {},
        "Soulard": {},
        "South Grand": {},
        "The Delmar Loop": {},
        "The Grove": {},
        "The Hill": {},
        "The Ville": {},
        "Webster Groves": {},
        "Washington University in St. Louis": {}
    }
    # Generate Feb 01–Feb 28 and Mar 01–Mar 31 entries
    def generate_daily_entries():
        days = []
        for month, days_in_month in [(2, 28), (3, 31)]:
            for day in range(1, days_in_month+1):
                d = f"{date(2023, month, day):%b %d %Y}"
                days.append(d)
                days.append(f"{d} - Neutral")
                days.append(f"{d} - Negative")
                days.append(f"{d} - Positive")

        return days

    value_keys = [
        "Total", "Total - Neutral", "Total - Negative", "Total - Positive",
        "Feb", "Feb - Neutral", "Feb - Negative", "Feb - Positive",
        "Mar", "Mar - Neutral", "Mar - Negative", "Mar - Positive",
        "Feb 01 - Feb 07 2023", "Feb 01 - Feb 07 2023 - Neutral", "Feb 01 - Feb 07 2023 - Negative", "Feb 01 - Feb 07 2023 - Positive",
        "Feb 08 - Feb 14 2023", "Feb 08 - Feb 14 2023 - Neutral", "Feb 08 - Feb 14 2023 - Negative", "Feb 08 - Feb 14 2023 - Positive",
        "Feb 15 - Feb 21 2023", "Feb 15 - Feb 21 2023 - Neutral", "Feb 15 - Feb 21 2023 - Negative", "Feb 15 - Feb 21 2023 - Positive",
        "Feb 22 - Feb 28 2023", "Feb 22 - Feb 28 2023 - Neutral", "Feb 22 - Feb 28 2023 - Negative", "Feb 22 - Feb 28 2023 - Positive",
        "Mar 01 - Mar 07 2023", "Mar 01 - Mar 07 2023 - Neutral", "Mar 01 - Mar 07 2023 - Negative", "Mar 01 - Mar 07 2023 - Positive",
        "Mar 08 - Mar 14 2023", "Mar 08 - Mar 14 2023 - Neutral", "Mar 08 - Mar 14 2023 - Negative", "Mar 08 - Mar 14 2023 - Positive",
        "Mar 15 - Mar 21 2023", "Mar 15 - Mar 21 2023 - Neutral", "Mar 15 - Mar 21 2023 - Negative", "Mar 15 - Mar 21 2023 - Positive",
        "Mar 22 - Mar 28 2023", "Mar 22 - Mar 28 2023 - Neutral", "Mar 22 - Mar 28 2023 - Negative", "Mar 22 - Mar 28 2023 - Positive",
        "Mar 29 - Apr 04 2023", "Mar 29 - Apr 04 2023 - Neutral", "Mar 29 - Apr 04 2023 - Negative", "Mar 29 - Apr 04 2023 - Positive"
    ] + generate_daily_entries()

    for location in locations:
        for value in value_keys:
            locations[location][value] = 0

    return locations

def get_date_range_key(month, day):
    assert day >= 1
    bins = [1, 8, 15, 22, 29]
    indx = 4
    while day < bins[indx]:
        indx -= 1

    if indx == 4:
        return "Mar 29 - Apr 04 2023"

    mon = "Feb" if month == 2 else "Mar"
    return f"{mon} {str(bins[indx]).zfill(2)} - {mon} {str(bins[indx+1]-1).zfill(2)} 2023"

# canonical list
CANON = {
    "St. Louis","Carondelet","Central West End","Cherokee Antique Row","Cherokee Street",
    "Chesterfield","Clayton","Downtown St. Louis","Eureka","Forest Park","Grand Center Arts District",
    "Kimmswick","Kirkwood","Laclede's Landing","Lafayette Square","Maplewood","Maryland Heights",
    "North County","Soulard","South Grand","The Delmar Loop","The Grove","The Hill","The Ville",
    "Webster Groves","Washington University in St. Louis"
}

# common local aliases -> canonical
ALIASES = {
    "Forest Park Southeast": "The Grove",
    "Downtown": "Downtown St. Louis",
    "Downtown West": "Downtown St. Louis",
    "CWE": "Central West End",
    "WashU": "Washington University in St. Louis",
    "Washington University": "Washington University in St. Louis",
    "Lacledes Landing": "Laclede's Landing",
}

def canonize(name: str | None) -> str | None:
    if not isinstance(name, str): 
        return None

    s = name.strip().title()
    if s in CANON: 
        return s

    if s in ALIASES: 
        s = ALIASES[s]
        return s if s in CANON else None

    s2 = s.replace("St Louis", "St. Louis")
    s2 = s2.replace("Saint Louis", "St. Louis")
    return s2 if s2 in CANON else None

def pick_neighborhood(g, n, poi, n_address) -> str | None:
    loc = canonize(g)
    if loc:
        return loc

    loc = canonize(n)
    if loc:
        return loc

    hay = " | ".join([
        str(poi or ""),
        str(n_address or "")
    ])
    for cand in sorted(CANON | set(ALIASES.keys()), key=len, reverse=True):
        if cand.lower() in hay.lower():
            return canonize(cand)

    return None

def filter_df(df, filter=False):
    if not filter:
        return df

    # get only rows with location_type = LatLon
    return df[df["location_type"] == "LatLon"]

def main(file, filter):
    df = filter_df(pd.read_csv(file), filter)
    res = create_dict()
    skip = 0
    total = 0 
    for (row_id,time_stamp,location_type,lat,lon,latlon,google_address,google_zip_code,google_city,google_country,
        google_state,google_nearest_poi,nominatim_address,nominatim_zip_code,nominatim_city,nominatim_country,
        nominatim_state,sentiment,google_neighborhood,nhood_best) in df.itertuples(index=False, name=None):
        total += 1
        location = pick_neighborhood(google_neighborhood, nhood_best, google_nearest_poi, nominatim_address)
        if location is None:
            skip += 1
            continue

        dt = datetime.strptime(time_stamp, "%m/%d/%y %H:%M")
        month, day, year = dt.month, dt.day, dt.year

        # We only care about dates in February or March 2023, with the exception of April 1-4th 2023
        if year != 2023 or month not in [2, 3] and not (month == 4 and day < 5):
            skip += 1
            continue

        date_range_key = get_date_range_key(month, day)
        date_key = ("Feb " if month == 2 else "Mar ") + str(day).zfill(2) + " 2023"

        res[location][date_range_key] += 1

        # No "April {day} 2023" column
        if month != 4:
            res[location][date_key] += 1
            res[location][date_key[:-8]] += 1
            
        if isinstance(sentiment, str):
            sentiment = sentiment.capitalize()
            if sentiment in ["Neutral", "Positive", "Negative"]:
                res[location][date_range_key + " - " + str(sentiment)] += 1
                res[location][date_key + " - " + str(sentiment)] += 1
                res[location][date_key[:-8] + " - " + str(sentiment)] += 1
                res[location]["Total - " + str(sentiment)] += 1

        res[location]["Total"] += 1
        
    print(skip, total)

    """
    out = []
    for loc, metrics in res.items():
        for k, v in metrics.items():
            out.append({"location": loc, "metric": k, "count": v})

    pd.DataFrame(out).to_csv("data/summary_counts.csv", index=False)
    """

    wide = pd.DataFrame(res) # index = metrics, columns = locations
    wide.index.name = "metric" # name the row index for the CSV
    wide = wide.reindex(columns=sorted(wide.columns)) # consistent column order
    wide.to_csv(f"data/summary_counts_latlon{'_place' if not filter else ''}.csv")

if __name__ == "__main__":
    args = parser.parse_args()
    main(args.file, args.filter)


"""
wc -l "$REPO/pipelines/rapids/data/interim/$PID/phone_locations_processed_with_datetime_with_doryab_columns_episodes.csv"
wc -l "$REPO/pipelines/rapids/data/interim/$PID/phone_locations_processed_with_datetime_with_doryab_columns_episodes_resampled.csv"
wc -l "$REPO/pipelines/rapids/data/interim/$PID/phone_locations_processed_with_datetime_with_doryab_columns_episodes_resampled_with_datetime.csv"

wc -l "$REPO/pipelines/rapids/data/interim/time_segments/${PID}_time_segments.csv"
wc -l "$REPO/pipelines/rapids/data/interim/time_segments/${PID}_time_segments_labels.csv"
head -n 3 "$REPO/pipelines/rapids/data/interim/time_segments/${PID}_time_segments_labels.csv"
"""