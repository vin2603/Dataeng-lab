import pandas as pd
import os

# Load the dataset
file_path = '/home/vincle/data-validation/employees.csv'
df = pd.read_csv(file_path)

# Get the size of the file in bytes
file_size_bytes = os.path.getsize(file_path)

# Get the number of records (rows)
num_records = len(df)

file_size_bytes, num_records

print(f"File size: {file_size_bytes} bytes")
