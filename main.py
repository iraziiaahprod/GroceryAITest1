"""
Flashfood Product Scraper - Supabase Integration
Fetches product data from Flashfood API and stores it in Supabase database
"""

from supabase import create_client
import pandas as pd
import json
import requests as rq
from dotenv import load_dotenv
import os
from time import sleep

# Load environment variables from .env file (for local development only)
if os.path.exists('.env'):
    load_dotenv()
    print("✓ Loaded .env file for local development")

# Initialize configuration from environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
FLASHFOOD_API_KEY = os.getenv('FLASHFOOD_API_KEY')

# Table name (hardcoded)
TABLE_NAME = 'scrape_results'

# Validate all required credentials are present
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials. Please check your environment variables.")
if not FLASHFOOD_API_KEY:
    raise ValueError("Missing Flashfood API key. Please check your environment variables.")

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Flashfood API configuration
api_url = 'https://app.shopper.flashfood.com/api/v1/items/'
headers = {
    'x-ff-api-key': FLASHFOOD_API_KEY
}
params = {
    'storeIds': [
        '6854601a36c2d6774d41f443', #Little Harvest Market
        '5ccc4fa913ed23170c14dae7', #Loblaws Maple Leaf Gardens
        '5fa2e7544d6a672e1136bfd1', #NoFrills Rocco's
        '5ccc4fa913ed23170c14dae2', #Loblaws Queens Quay
        '68d2c2a1765cb78193ff5f47' #NoFrills Esplanade
    ]
}

# Fetch data from Flashfood API with retry logic
print("=" * 50)
print("Fetching data from Flashfood API...")

max_retries = 3
data_res = None

for attempt in range(max_retries):
    try:
        response = rq.get(api_url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data_res = response.json()
        print(f"✓ Successfully fetched data from API (attempt {attempt + 1})")
        break
    except rq.exceptions.RequestException as e:
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
            print(f"⚠ Attempt {attempt + 1} failed: {e}")
            print(f"  Retrying in {wait_time} seconds...")
            sleep(wait_time)
        else:
            print(f"✗ All {max_retries} attempts failed: {e}")
            print(f"  Error type: {type(e).__name__}")

            # Optional: Fallback to local file for development/testing
            # Uncomment the lines below if you want to use cached data locally
            # if os.path.exists('response.json'):
            #     print("  Attempting to load from local response.json file...")
            #     try:
            #         with open('response.json', 'r') as file:
            #             data_res = json.load(file)
            #         print("✓ Loaded data from local file")
            #         break
            #     except Exception as file_error:
            #         print(f"✗ Failed to load local file: {file_error}")

            raise  # Re-raise the exception to fail the workflow

# Verify we have data
if data_res is None:
    print("✗ No data available. Exiting.")
    exit(1)

# Get all the data from 'data' (store id mapping)
stores = data_res.get('data', {})

if not stores:
    print("✗ No store data found in API response")
    print(f"API response keys: {data_res.keys()}")
    exit(1)

# Create a list to store all products
all_columns = []

# Get current timestamp
current_timestamp = pd.Timestamp.today()

# Loop through the nested fields in json file
for storeIDs, products in stores.items():
    if isinstance(products, list):
        for product in products:
            # Add timestamp to each product (lowercase to match Supabase column naming)
            product['scraper_timestamp'] = current_timestamp
            all_columns.append(product)
    else:
        print(f"⚠ Warning: Products for store {storeIDs} is not a list")

# Create DataFrame
df = pd.DataFrame(all_columns)

# Convert all column names to lowercase to match PostgreSQL conventions
df.columns = df.columns.str.lower()

print("\nDataFrame Info:")
print(df.info())
print(f"\nTotal products found: {len(df)}")

# Check if DataFrame is empty
if df.empty:
    print("✗ No products found in API response. Exiting.")
    exit(0)

# Data type conversions and cleaning
print("\nProcessing data...")

# Convert scraper_timestamp to ISO format string if it exists
if 'scraper_timestamp' in df.columns:
    df['scraper_timestamp'] = pd.to_datetime(df['scraper_timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
else:
    print("⚠ Warning: scraper_timestamp column not found in data")
    print(f"Available columns: {df.columns.tolist()}")

# Safe type conversions with error handling
if 'originalprice' in df.columns:
    df['originalprice'] = pd.to_numeric(df['originalprice'], errors='coerce')
if 'price' in df.columns:
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
if 'bestbeforedate' in df.columns:
    df['bestbeforedate'] = df['bestbeforedate'].astype('int64')
if 'quantityavailable' in df.columns:
    df['quantityavailable'] = df['quantityavailable'].astype('int64')

# imageGallery is already a list from JSON, ensure it's properly formatted
if 'imagegallery' in df.columns:
    df['imagegallery'] = df['imagegallery'].apply(
        lambda x: x if isinstance(x, list) else [x] if x else []
    )

# Convert to records, handling NaN values
data = df.where(pd.notna(df), None).to_dict('records')

# Debug: Print first record to see what we're sending
if data:
    print(f"\nSample record (first product):")
    print(json.dumps(data[0], indent=2, default=str))

print(f"\nInserting {len(data)} products into Supabase table '{TABLE_NAME}'...")

# Insert data in batches
batch_size = 1000
total_inserted = 0
errors = []

for i in range(0, len(data), batch_size):
    batch = data[i:i + batch_size]
    batch_num = i // batch_size + 1

    try:
        response = supabase.table(TABLE_NAME).insert(batch).execute()
        total_inserted += len(batch)
        print(f"✓ Inserted batch {batch_num}: {len(batch)} rows")
    except Exception as e:
        error_msg = f"Batch {batch_num}: {str(e)}"
        errors.append(error_msg)
        print(f"✗ Error inserting batch {batch_num}: {e}")
        print(f"   Error type: {type(e).__name__}")

        # Print first record of failed batch for debugging
        if batch:
            print(f"   First record in failed batch:")
            print(f"   {json.dumps(batch[0], indent=2, default=str)[:500]}...")  # Limit output

print(f"\n{'='*50}")
print(f"Total rows inserted: {total_inserted}/{len(data)}")

if errors:
    print(f"\n✗ Errors encountered: {len(errors)}")
    for error in errors:
        print(f"  - {error}")
    print("\n✗ Script completed with errors")
    exit(1)  # Exit with error code for GitHub Actions
else:
    print("\n✓ All data inserted successfully into Supabase!")
    print("✓ Script completed successfully")
    exit(0)  # Exit successfully

# Optional: Save backup CSV
# df.to_csv('Data_cleaned.csv', index=False)
# print("\n✓ Backup CSV saved as 'Data_cleaned.csv'")