import os
import pandas as pd
from typing import Dict
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from tqdm import tqdm

CHUNK_SIZE = 500_000  # batch size, adjust for your memory

class DataCleaner:
    def __init__(self):
        """Initialize PostgreSQL and MongoDB connections."""
        self.postgres_engine = self._get_postgres_engine()
        self.mongo_client = self._get_mongo_client()
        self.mongo_db = self.mongo_client["nyc_taxi"]
        self.collection = self.mongo_db["cleaned_trips"]
        print("✅ Connections initialized successfully (PostgreSQL & MongoDB)")

    # ----------------------------
    # Connections
    # ----------------------------
    def _get_postgres_engine(self):
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "nyc_taxi")

        url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        print(f"PostgreSQL connection: {url}")
        return create_engine(url)

    def _get_mongo_client(self):
        mongo_user = os.getenv("MONGO_USER", "admin")
        mongo_password = os.getenv("MONGO_PASSWORD", "admin")
        mongo_host = os.getenv("MONGO_HOST", "mongodb")
        mongo_port = os.getenv("MONGO_PORT", "27017")
        mongo_url = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/"
        print(f"🔗 MongoDB connection: {mongo_url}")
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
        client.server_info()  # force connection check
        return client

    # ----------------------------
    # Cleaning rules
    # ----------------------------
    def clean_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply cleaning rules to a single DataFrame chunk."""
        for col in ["passenger_count", "trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount"]:
            if col in df.columns:
                df = df[df[col] >= 0]

        if "passenger_count" in df.columns:
            df = df[(df["passenger_count"] >= 1) & (df["passenger_count"] <= 8)]
        if "trip_distance" in df.columns:
            df = df[df["trip_distance"] <= 100]
        if "fare_amount" in df.columns:
            df = df[df["fare_amount"] <= 500]

        # Handle date columns
        pickup_col = "pickup_datetime" if "pickup_datetime" in df.columns else "tpep_pickup_datetime"
        dropoff_col = "dropoff_datetime" if "dropoff_datetime" in df.columns else "tpep_dropoff_datetime"
        df = df.dropna(subset=[c for c in [pickup_col, dropoff_col] if c in df.columns])

        return df

    # ----------------------------
    # Batch processing
    # ----------------------------
    def process_batches(self):
        """Process PostgreSQL table in batches and save cleaned data to MongoDB."""
        with self.postgres_engine.connect() as conn:
            total_rows = conn.execute(text("SELECT COUNT(*) FROM yellow_taxi_trips")).scalar()
            print(f"Total rows in PostgreSQL: {total_rows}")

            offset = 0
            pbar = tqdm(total=total_rows, desc="Processing batches", unit="rows")

            # Clear existing MongoDB data
            existing_count = self.collection.count_documents({})
            if existing_count > 0:
                self.collection.delete_many({})

            while offset < total_rows:
                query = text(f"""
                    SELECT * FROM yellow_taxi_trips
                    ORDER BY id
                    LIMIT {CHUNK_SIZE} OFFSET {offset}
                """)
                df_chunk = pd.read_sql(query, conn)
                cleaned_df = self.clean_chunk(df_chunk)

                if not cleaned_df.empty:
                    for col in ["pickup_datetime", "dropoff_datetime"]:
                        if col in cleaned_df.columns:
                            cleaned_df[col] = pd.to_datetime(cleaned_df[col])
                    records = cleaned_df.to_dict(orient="records")
                    self.collection.insert_many(records)

                offset += CHUNK_SIZE
                pbar.update(len(df_chunk))
            pbar.close()
        print("✅ Batch cleaning complete!")

    # ----------------------------
    # Close MongoDB
    # ----------------------------
    def close(self):
        self.mongo_client.close()
        print("MongoDB connection closed.")


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    cleaner = DataCleaner()
    try:
        cleaner.process_batches()
    finally:
        cleaner.close()
