import duckdb
from pathlib import Path
from datetime import datetime
import os


class DuckDBImporter:
    def __init__(self, db_path: str):
        """Initialize DuckDB connection and create tables if needed."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._initialize_database()

    def _initialize_database(self):
        """Create required tables if they do not exist."""
        # Main trips table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
                VendorID BIGINT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count DOUBLE,
                trip_distance DOUBLE,
                RatecodeID DOUBLE,
                store_and_fwd_flag VARCHAR,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                payment_type BIGINT,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                congestion_surcharge DOUBLE,
                Airport_fee DOUBLE
            )
        """)

        # Import log table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                file_name VARCHAR PRIMARY KEY,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_imported BIGINT
            )
        """)

    def is_file_imported(self, filename: str) -> bool:
        """Check if a given file has already been imported."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM import_log WHERE file_name = ?", [filename]
        ).fetchone()
        return result[0] > 0

    def import_parquet(self, file_path: Path) -> bool:
        """Import a Parquet file into the yellow_taxi_trips table (auto-aligns columns)."""
        filename = file_path.name

        if self.is_file_imported(filename):
            print(f"✅ {filename} already imported, skipping.")
            return True

        try:
            before_count = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()[0]

            # Dynamically detect common columns
            table_cols = [
                r[1]
                for r in self.conn.execute(
                    "PRAGMA table_info('yellow_taxi_trips')"
                ).fetchall()
            ]
            parquet_cols = [
                desc[0]
                for desc in self.conn.execute(
                    f"SELECT * FROM read_parquet('{file_path}') LIMIT 0"
                ).description
            ]
            common_cols = [c for c in parquet_cols if c in table_cols]

            if not common_cols:
                raise ValueError(
                    f"No matching columns found between table and file {filename}"
                )

            col_list = ", ".join(common_cols)

            print(f"Importing {filename} ({len(common_cols)} matching columns)...")
            self.conn.execute(f"""
                INSERT INTO yellow_taxi_trips ({col_list})
                SELECT {col_list} FROM read_parquet('{file_path}')
            """)

            after_count = self.conn.execute(
                "SELECT COUNT(*) FROM yellow_taxi_trips"
            ).fetchone()[0]
            rows_imported = after_count - before_count

            self.conn.execute(
                """
                INSERT INTO import_log (file_name, import_date, rows_imported)
                VALUES (?, ?, ?)
            """,
                [filename, datetime.now(), rows_imported],
            )

            print(f"{filename} imported successfully ({rows_imported} rows).")
            return True

        except Exception as e:
            print(f"Error importing {filename}: {e}")
            return False

    def import_all_parquet_files(self, data_dir: Path) -> int:
        """Import all Parquet files from the specified directory."""
        parquet_files = sorted(data_dir.glob("*.parquet"))
        if not parquet_files:
            print("No .parquet files found in the directory.")
            return 0

        imported_count = 0
        for file in parquet_files:
            if self.import_parquet(file):
                imported_count += 1

        print(f"Imported {imported_count}/{len(parquet_files)} files successfully.")
        return imported_count

    def get_statistics(self):
        """Display basic statistics about the imported data."""
        total_trips = self.conn.execute(
            "SELECT COUNT(*) FROM yellow_taxi_trips"
        ).fetchone()[0]
        imported_files = self.conn.execute(
            "SELECT COUNT(*) FROM import_log"
        ).fetchone()[0]

        # Check date range if data exists
        date_range = self.conn.execute("""
            SELECT 
                MIN(tpep_pickup_datetime), 
                MAX(tpep_pickup_datetime)
            FROM yellow_taxi_trips
        """).fetchone()

        db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024)

        print("\nDuckDB Database Statistics")
        print(f" - Total trips        : {total_trips:,}")
        print(f" - Files imported     : {imported_files}")
        print(f" - Pickup date range  : {date_range[0]} → {date_range[1]}")
        print(f" - Database size      : {db_size_mb:.2f} MB")

    def close(self):
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            print("🔒 DuckDB connection closed.")
