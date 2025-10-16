from src.download_data import NYCTaxiDataDownloader
from src.import_to_duckdb import DuckDBImporter
from pathlib import Path

if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader(year=2025, data_dir="data/raw")
    downloader.download_all_available()
    importer = DuckDBImporter("yellow_taxi.duckdb")
    importer.import_all_parquet_files(Path("data/raw"))
    importer.get_statistics()
    importer.close()
