from src.extract import extract_transactions, extract_profiles
from src.clean import clean_transactions, clean_profiles

data_transactions = extract_transactions()
try:
    data_profiles = extract_profiles()
except ValueError as e:
    print("Misformatted JSON file: ", e)
    exit(1)

print(data_transactions)
print(data_profiles)

# Clean the data
data_transactions = clean_transactions(data_transactions)
data_profiles = clean_profiles(data_profiles)

print(data_transactions)
print(data_profiles)