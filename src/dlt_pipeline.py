import dlt
import pandas as pd
from pathlib import Path
from typing import Iterator, Dict, Any
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import gc
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class NYCTaxiDLTPipeline:
    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    DATA_DIR = Path("data")
    MAX_WORKERS = 4
    BATCH_SIZE = 5000

    def __init__(self):
        """Initialize pipeline and determine latest available year/month."""
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.YEAR, self.months = self._get_available_months()
        logging.info(f"Initialized NYCTaxiDLTPipeline for year {self.YEAR}, months {self.months}")

    def _get_available_months(self) -> tuple[int, list[int]]:
        """
        Automatically detect the latest year with available NYC Taxi data.
        Tests from current year backward until a valid month is found.
        """
        current_year = datetime.now().year
        for year in range(current_year, 2020, -1):
            url = f"{self.BASE_URL}/yellow_tripdata_{year}-01.parquet"
            resp = requests.head(url)
            if resp.status_code == 200:
                months = []
                for month in range(1, 13):
                    url = f"{self.BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"
                    resp = requests.head(url)
                    if resp.status_code == 200:
                        months.append(month)
                return year, months
        raise RuntimeError("No valid NYC Taxi dataset found online.")

    def _download_if_needed(self, month: int) -> Path | None:
        file_name = f"yellow_tripdata_{self.YEAR}-{month:02d}.parquet"
        file_path = self.DATA_DIR / file_name
        if not file_path.exists():
            url = f"{self.BASE_URL}/{file_name}"
            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                file_path.write_bytes(r.content)
            except requests.HTTPError as e:
                logging.warning(f"Skipping {url}: {e}")
                return None
            except requests.RequestException as e:
                logging.error(f"Network error for {url}: {e}")
                return None
        return file_path

    def download_all(self, months: list[int]) -> list[Path]:
        """Download all months in parallel and show tqdm progress."""
        results = []
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = {executor.submit(self._download_if_needed, m): m for m in months}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading files"):
                path = future.result()
                if path:
                    results.append(path)
        return results

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["passenger_count", "trip_distance"])
        df = df[df["trip_distance"] > 0]
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        return df

    def get_resource(self):
        @dlt.resource(name="yellow_taxi_trips", write_disposition="append")
        def load_taxi_data() -> Iterator[Dict[str, Any]]:
            file_paths = self.download_all(self.months)

            for file_path in tqdm(file_paths, desc="Processing files"):
                df = pd.read_parquet(file_path)
                df = self._clean_data(df)
                records = df.to_dict("records")

                for record in records:
                    yield record

                del df, records
                gc.collect()

            tqdm._instances.clear()

        return load_taxi_data

    def run_pipeline(self, destination="postgres"):
        pipeline = dlt.pipeline(
            pipeline_name="nyc_taxi_pipeline",
            destination=destination,
            dataset_name="nyc_taxi_dlt",
            full_refresh=False
        )

        resource = self.get_resource()
        gen = resource()

        try:
            load_info = pipeline.run(gen)
            logging.info(load_info)
        finally:
            if hasattr(gen, "close"):
                gen.close()
            pipeline.close()
            tqdm._instances.clear()
            gc.collect()

        return load_info


if __name__ == "__main__":
    pipeline = NYCTaxiDLTPipeline()
    pipeline.run_pipeline()