import pandas as pd
from sodapy import Socrata

from scripts import config
from scripts.logger import logger


def download_data(file: str, file_name: str) -> None:
    client = Socrata("finances.worldbank.org", None)
    results = client.get(file, limit=2_000_000)
    pd.DataFrame.from_records(results).to_feather(
        config.PATHS.raw_data / f"{file_name}_data.feather"
    )

    logger.info(f"Downloaded {file_name}_data")

