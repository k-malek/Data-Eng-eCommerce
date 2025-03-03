from src.extract import extract_transactions, extract_profiles
from src.clean import clean_transactions, clean_profiles
from src.transform import consolidate_data
from tabulate import tabulate

# Extract the data
data_transactions = extract_transactions()
try:
    data_profiles = extract_profiles()
except ValueError as e:
    print("Misformatted JSON file: ", e)
    exit(1)

# Clean the data
data_transactions = clean_transactions(data_transactions)
data_profiles = clean_profiles(data_profiles)

# Transform the data
data = consolidate_data(data_transactions, data_profiles)
print(tabulate(data, headers='keys', tablefmt='fancy_grid'))