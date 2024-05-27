import pandas as pd
from moralis import evm_api
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Set your Moralis API key
api_key = "YOUR_API_KEY"

def get_wallet_transaction_history(address, chain, order="ASC", limit=300, retries=3):
    params = {
        "address": address,
        "chain": chain,
        "order": order,
        "limit": limit
    }
    
    for attempt in range(retries):
        try:
            result = evm_api.transaction.get_wallet_transactions(api_key=api_key, params=params)
            return result
        except Exception as e:
            print(f"Error fetching data for {address} on {chain}: {e}")
            if attempt < retries - 1:
                print(f"Retrying... ({attempt + 1}/{retries})")
                time.sleep(5)  # Delay before retrying
            else:
                return None

def extract_data(transactions):
    if transactions and 'result' in transactions and transactions['result']:
        first_tx = transactions['result'][0]
        last_tx = transactions['result'][-1]
        return {
            "first_transaction_date": first_tx['block_timestamp'],
            "last_transaction_date": last_tx['block_timestamp'],
            "transaction_count": len(transactions['result']),
            "first_transaction": first_tx
        }
    else:
        return {
            "first_transaction_date": None,
            "last_transaction_date": None,
            "transaction_count": 0,
            "first_transaction": None
        }

def process_wallet(address):
    chains = ["eth", "polygon", "bsc", "base", "arbitrum", "fantom", "avalanche"]
    data = {"address": address}
    earliest_tx = None

    with ThreadPoolExecutor(max_workers=len(chains)) as executor:
        futures = {executor.submit(get_wallet_transaction_history, address, chain): chain for chain in chains}
        for future in as_completed(futures):
            chain = futures[future]
            transactions = future.result()
            if transactions:
                chain_data = extract_data(transactions)
                data[f"{chain}_transaction_count"] = chain_data["transaction_count"]
                data[f"{chain}_first_transaction_date"] = chain_data["first_transaction_date"]
                data[f"{chain}_last_transaction_date"] = chain_data["last_transaction_date"]
                if chain_data["first_transaction_date"] and (earliest_tx is None or chain_data["first_transaction_date"] < earliest_tx["block_timestamp"]):
                    earliest_tx = chain_data["first_transaction"]
            else:
                data[f"{chain}_transaction_count"] = 0
                data[f"{chain}_first_transaction_date"] = None
                data[f"{chain}_last_transaction_date"] = None

    if earliest_tx:
        funding_source = earliest_tx.get("from_address_label")
        if not funding_source:
            funding_source = earliest_tx.get("from_address")
        data["funding_source"] = funding_source
    else:
        data["funding_source"] = None

    return data

# Read addresses from file
with open("addresses.txt", "r") as file:
    addresses = file.read().splitlines()

# Program start time
start_time = datetime.now()
print(f"Starting search at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

all_data = []

# Process each wallet in parallel, no more than 10 at the same time
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(process_wallet, address): address for address in addresses}
    for future in as_completed(futures):
        address = futures[future]
        wallet_data = future.result()
        all_data.append(wallet_data)

# Create DataFrame from collected data
df = pd.DataFrame(all_data)

# Convert dates to timezone-unaware format
for chain in ["eth", "polygon", "bsc", "base", "arbitrum", "fantom", "avalanche"]:
    df[f"{chain}_first_transaction_date"] = pd.to_datetime(df[f"{chain}_first_transaction_date"]).dt.tz_localize(None)
    df[f"{chain}_last_transaction_date"] = pd.to_datetime(df[f"{chain}_last_transaction_date"]).dt.tz_localize(None)

# Find wallet registration date for each address
df['registration_date'] = df[[f"{chain}_first_transaction_date" for chain in ["eth", "polygon", "bsc", "base", "arbitrum", "fantom", "avalanche"]]].min(axis=1)

# Reorder columns
columns = ["address", "registration_date", "funding_source"]
for chain in ["eth", "polygon", "bsc", "base", "arbitrum", "fantom", "avalanche"]:
    columns.extend([f"{chain}_transaction_count", f"{chain}_first_transaction_date", f"{chain}_last_transaction_date"])

df = df[columns]

# Sort by registration date
df = df.sort_values(by='registration_date')

# Save data to Excel
df.to_excel("wallet_transaction_history.xlsx", index=False)

# Program end time
end_time = datetime.now()
print(f"Data successfully saved to wallet_transaction_history.xlsx at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Execution time: {end_time - start_time}")
