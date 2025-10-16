from src.download_data import NYCTaxiDataDownloader

if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader(year=2025, data_dir="data/raw")
    downloader.download_all_available()

