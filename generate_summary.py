import argparse
import pandas as pd
from datetime import date, datetime

# python3 generate_summary.py --file "data/gps_points_geocoded.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--file", '-f', type=str, required=True, help="Path to reversed geocode csv file")

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
        "March", "March - Neutral", "March - Negative", "March - Positive",
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

def main(file):
    df = pd.read_csv(file)
    res = create_dict()
    for (row_id,time_stamp,lat,lon,latlon,google_address,google_zip_code,google_city,google_country,
        google_state,google_nearest_poi,nominatim_address,nominatim_zip_code,nominatim_city,nominatim_country,
        nominatim_state,sentiment,google_neighborhood,nhood_best) in df.itertuples(index=False, name=None):

        if google_neighborhood in res:
            location = google_neighborhood

        elif nhood_best in res:
            location = nhood_best

        else:
            print("Location outside of queried set")
            continue

        sentiment = sentiment.capitalize()
        date = datetime.strptime(time_stamp, "%m/%d/%y %H:%M")
        month, day, year = date.month, date.day, date.year
        date_range_key = get_date_range_key(month, day)
        date_key = ("Feb " if month == 2 else "Mar ") + str(day).zfill(2) + " 2023"

        res[location][date_range_key] += 1
        res[location][date_key] += 1
        res[location][date_range_key + " - " + str(sentiment)] += 1
        res[location][date_key + " - " + str(sentiment)] += 1
        res[location][date_key[:-7]] += 1
        res[location][date_key[:-7] + " - " + str(sentiment)] += 1

        res[location]["Total"] += 1
        res[location]["Total - " + str(sentiment)] += 1
        

if __name__ == "__main__":
    args = parser.parse_args()

    main(args.file)