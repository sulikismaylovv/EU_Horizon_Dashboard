from supabase import create_client
from backend.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def upsert_data(table, records, conflict_key, batch_size=200):
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        res = supabase.table(table).upsert(batch, on_conflict=conflict_key).execute()
        if getattr(res, "status_code", None) not in (200, 201):
            print(f"❌ Error upserting to {table}: {getattr(res, 'data', res)}")

def insert_data(table, records, batch_size=200):
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        res = supabase.table(table).insert(batch).execute()
        if getattr(res, "status_code", None) not in (200, 201):
            print(f"❌ Error inserting to {table}: {getattr(res, 'data', res)}")
