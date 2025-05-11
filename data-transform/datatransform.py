# DataTransform.py

import pandas as pd
from datetime import datetime, timedelta

# Load the CSV file
#df = pd.read_csv("bc_trip259172515_230215.csv")

# Drop unwanted columns
#df = df.drop(columns=["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP"])

df = pd.read_csv("bc_trip259172515_230215.csv", usecols=lambda col: col not in ["EVENT_NO_STOP", "GPS_SATELLITES", "GPS_HDOP"])
# Combine OPD_DATE and ACT_TIME into a new TIMESTAMP column
def make_timestamp(row):
    # Match format like '15FEB2023:00:00:00'
    date = datetime.strptime(row['OPD_DATE'], "%d%b%Y:%H:%M:%S")
    seconds = int(row['ACT_TIME'])
    return date + timedelta(seconds=seconds)


df['TIMESTAMP'] = df.apply(make_timestamp, axis=1)

# Drop OPD_DATE and ACT_TIME columns
df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])
# Calculate differences
df['dMETERS'] = df['METERS'].diff()
df['dTIMESTAMP'] = df['TIMESTAMP'].diff().dt.total_seconds()

# Calculate SPEED = distance / time
df['SPEED'] = df.apply(lambda row: row['dMETERS'] / row['dTIMESTAMP'] if row['dTIMESTAMP'] else 0, axis=1)

# Drop temporary columns
df = df.drop(columns=['dMETERS', 'dTIMESTAMP'])

# Print speed statistics
print("Minimum speed:", df['SPEED'].min())
print("Maximum speed:", df['SPEED'].max())
print("Average speed:", df['SPEED'].mean())

print(df.head())
# Count and display the number of breadcrumb records
print("Number of breadcrumb records:", len(df))

