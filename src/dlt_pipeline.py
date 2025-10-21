import dlt
import pandas as pd
from pathlib import Path
from typing import Iterator, Dict, Any
import requests
import logging

logging.basicConfig(level=logging.INFO)


class NYCTaxiDLTPipeline:
    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    DATA_DIR = Path("data")
    YEAR = 2025

    def __init__(self):
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        logging.info("Initialized NYCTaxiDLTPipeline")

    def _download_if_needed(self, month: int) -> Path:
        file_name = f"yellow_tripdata_{self.YEAR}-{month:02d}.parquet"
        file_path = self.DATA_DIR / file_name
        if not file_path.exists():
            url = f"{self.BASE_URL}/{file_name}"
            logging.info(f"Downloading {url}")
            r = requests.get(url)
            r.raise_for_status()
            file_path.write_bytes(r.content)
        return file_path

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["passenger_count", "trip_distance"])
        df = df[df["trip_distance"] > 0]
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        return df

    def get_resource(self):
        """Return a DLT resource dynamically bound to this instance."""
        @dlt.resource(name="yellow_taxi_trips", write_disposition="append")
        def load_taxi_data() -> Iterator[Dict[str, Any]]:
            for month in range(1, 4):  # Example: first 3 months
                file_path = self._download_if_needed(month)
                df = pd.read_parquet(file_path)
                df = self._clean_data(df)
                logging.info(f"Processed data for month {month}")
                for record in df.to_dict("records"):
                    yield record

        return load_taxi_data

    def run_pipeline(self, destination="postgres"):
        pipeline = dlt.pipeline(
            pipeline_name="nyc_taxi_pipeline",
            destination=destination,
            dataset_name="nyc_taxi_dlt",
        )

        resource = self.get_resource()
        load_info = pipeline.run(resource())
        logging.info(load_info)
        return load_info


if __name__ == "__main__":
    pipeline = NYCTaxiDLTPipeline()
    pipeline.run_pipeline()
