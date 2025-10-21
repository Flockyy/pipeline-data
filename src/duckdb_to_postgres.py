import os
import duckdb
import psycopg2
import pandas as pd
from io import StringIO
from tqdm import tqdm

# Config
DUCKDB_FILE = os.getenv("DUCKDB_FILE", "yellow_taxi.duckdb")
TABLE_NAME = os.getenv("TABLE_NAME", "yellow_taxi_trips")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 500_000))

PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "nyc_taxi")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))

# Column mapping (DuckDB -> PostgreSQL)
COLUMN_MAPPING = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee"
}

# DuckDB connection
con = duckdb.connect(DUCKDB_FILE)

# PostgreSQL connection
pg_conn = psycopg2.connect(
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT
)

# Fetch total number of rows
total_rows = con.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]
print(f"Total rows in DuckDB: {total_rows}")

# Batch export
offset = 0
while offset < total_rows:
    df = con.execute(f"""
        SELECT * FROM yellow_taxi_trips
        LIMIT {CHUNK_SIZE} OFFSET {offset}
    """).df()

    # Rename columns
    df.rename(columns=COLUMN_MAPPING, inplace=True)

    # Cast integers
    int_cols = ["vendor_id", "passenger_count", "ratecode_id", "pu_location_id", "do_location_id", "payment_type"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Cast timestamps
    ts_cols = ["pickup_datetime", "dropoff_datetime"]
    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # CSV buffer for COPY
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
    csv_buffer.seek(0)

    # COPY into PostgreSQL
    with pg_conn.cursor() as cur:
        columns = df.columns.tolist()
        cur.copy_expert(f"COPY {TABLE_NAME} ({', '.join(columns)}) FROM STDIN WITH CSV", file=csv_buffer)
        pg_conn.commit()

    print(f"Inserted rows {offset + 1} to {offset + len(df)}")
    offset += CHUNK_SIZE

pg_conn.close()
con.close()
print("DuckDB -> PostgreSQL export complete!")
