import pandas as pd
import matplotlib.pyplot as plt

# Load the dataset
file_path = '/home/vincle/data-validation/employees.csv'
df = pd.read_csv(file_path)

#Check if 'name' field is missing
missing_name_count = df['name'].isnull().sum()

print(f"Records with missing 'name': {missing_name_count}")

# Assertion: every record must have a non-null 'eid' field
missing_eid_count = df['eid'].isnull().sum()

print(f"Records with missing 'eid': {missing_eid_count}")

df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')

# Count records where hire_date is before 2015
violating_hire_dates = df[df['hire_date'].dt.year < 2015]
hire_date_violation_count = len(violating_hire_dates)

print(f"Records hired before 2015: {hire_date_violation_count}")


df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')

# Find violations: birth_date AFTER hire_date
birth_after_hire = df[df['birth_date'] > df['hire_date']]
birth_after_hire_count = len(birth_after_hire)

print(f"Records where birth_date is after hire_date: {birth_after_hire_count}")

all_eids = set(df['eid'])

# Find violations: reports_to not in eids (ignore if reports_to is null)
invalid_managers = df[(df['reports_to'].notnull()) & (~df['reports_to'].isin(all_eids))]
invalid_manager_count = len(invalid_managers)

print(f"Records where manager (reports_to) is unknown: {invalid_manager_count}")


city_counts = df['city'].value_counts()

# Find cities with only 1 employee
cities_with_one_employee = city_counts[city_counts == 1]
cities_with_one_employee_count = len(cities_with_one_employee)

print(f"Cities with only one employee: {cities_with_one_employee_count}")

# Plot histogram of salaries
plt.hist(df['salary'].dropna(), bins=50)
plt.xlim(0,500000)
plt.title('Salary Distribution')
plt.xlabel('Salary')
plt.ylabel('Frequency')
plt.grid(True)
plt.savefig('salary_histogram.png') 

