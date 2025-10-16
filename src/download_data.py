import requests
from pathlib import Path
from datetime import datetime
from tqdm import tqdm  # Pretty progress bar


class NYCTaxiDataDownloader:
    def __init__(self, year: int = 2025, data_dir: str = "data/raw"):
        """
        Initialize constants and create the destination directory.
        All files will be saved in data/raw/.
        """
        self.BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        self.YEAR = year
        self.DATA_DIR = Path(data_dir)
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)

    def get_file_path(self, month: int) -> Path:
        """Build the full file path for a given month."""
        filename = f"yellow_tripdata_{self.YEAR}-{month:02d}.parquet"
        return self.DATA_DIR / filename

    def file_exists(self, month: int) -> bool:
        """Check if the file already exists locally."""
        return self.get_file_path(month).exists()

    def download_month(self, month: int) -> bool:
        """
        Download the dataset for a given month if it’s not already present.
        """
        file_path = self.get_file_path(month)

        # Skip existing files
        if self.file_exists(month):
            print(f"✅ {file_path.name} already exists — skipping download.")
            return True

        url = f"{self.BASE_URL}/yellow_tripdata_{self.YEAR}-{month:02d}.parquet"
        print(f"⬇️ Downloading from {url}...")

        try:
            with requests.get(url, stream=True, timeout=30) as response:
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))
                chunk_size = 8192

                with open(file_path, "wb") as f, tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    desc=file_path.name,
                    ncols=80,
                    colour="green"
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            print(f"✅ Downloaded: {file_path.name}\n")
            return True

        except requests.exceptions.RequestException as e:
            print(f"❌ Error downloading {file_path.name}: {e}")
            # Delete partial file if an error occurs
            if file_path.exists():
                file_path.unlink()
            return False

    def download_all_available(self) -> list:
        """Download all available months up to the current month."""
        now = datetime.now()
        months = range(1, now.month + 1) if self.YEAR == now.year else range(1, 13)

        print(f"📦 Downloading NYC Yellow Taxi data for {self.YEAR}...\n")
        downloaded_files = []

        for month in months:
            if self.download_month(month):
                downloaded_files.append(self.get_file_path(month))

        print("\n📊 Summary:")
        for f in downloaded_files:
            print(f" - {f.name}")

        print(f"\n✅ {len(downloaded_files)} files available/downloaded successfully.")
        return downloaded_files
