import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

API = "https://earthquake.usgs.gov/fdsnws/event/1/query"

start = datetime(2016, 10, 4)
end = datetime(2026, 10, 11)

all_data = []
current = start

while current < end:
    next_month = current + relativedelta(months=1)
    chunk_end = min(next_month, end)

    params = {
        "format": "csv",
        "starttime": current.strftime("%Y-%m-%d"),
        "endtime": chunk_end.strftime("%Y-%m-%d"),
        "minmagnitude": 2.5,
        "orderby": "time-asc",
        "limit": 20000
    }

    response = requests.get(API, params=params, timeout=120)
    response.raise_for_status()

    from io import StringIO
    df = pd.read_csv(StringIO(response.text))

    if len(df) >= 20000:
        raise RuntimeError(
            f"Limit reached for {current:%Y-%m}. "
            "Split this period into smaller date ranges."
        )

    all_data.append(df)
    print(f"{current:%Y-%m}: {len(df)} records")

    current = chunk_end

result = pd.concat(all_data, ignore_index=True)
result = result.drop_duplicates(subset="id")

result.to_csv("quakes.csv", index=False)

print("Total records:", len(result))
print("Saved as quakes.csv")
