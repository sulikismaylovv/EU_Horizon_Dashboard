# Supabase client for database operations
# backend/db/supabase_client.py
from supabase import create_client
from backend.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# Example usage:
# def insert_data(table: str, data: list):
#     """Insert data into a Supabase table."""  
#     response = supabase.table(table).insert(data).execute()
#     if response.status_code == 201:
#         print(f"Inserted {len(data)} records into {table}")
#     else:
#         print(f"Error inserting data into {table}: {response.status_code} - {response.data}")
#         df = pd.read_csv(path, converters=converters, parse_dates=parse_dates)
