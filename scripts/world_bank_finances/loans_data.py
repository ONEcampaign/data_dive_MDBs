from functools import partial

import pandas as pd

from scripts import config
from scripts.world_bank_finances import download

file_name = "ibrd_historical_loan"

download_loan_data = partial(
    download.download_data, file="zucq-nrc3", file_name=file_name
)


def read_raw_data(dataset: str) -> pd.DataFrame:
    """Read the raw data from the WB"""
    return pd.read_feather(config.PATHS.raw_data / f"{dataset}_data.feather").assign(
        end_of_period=lambda d: pd.to_datetime(
            d.end_of_period, infer_datetime_format=True
        )
    )
