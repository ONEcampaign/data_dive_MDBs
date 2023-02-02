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


if __name__ == "__main__":
    status = [
        "Disbursed",
        "Repaying",
        "Disbursing",
        "Disbursing&Repaying",
        "Approved",
        "Effective",
    ]
    df = (
        read_raw_data(file_name)
        .query("loan_status in @status")
        .astype(
            {
                "interest_rate": float,
                "original_principal_amount": float,
                "disbursed_amount": float,
                "undisbursed_amount": float,
            }
        )
        .sort_values(
            by=["end_of_period", "loan_number", "interest_rate"],
            ascending=(False, True, False),
        )
        .drop_duplicates(subset=["loan_number", "country"], keep="first")
        .assign(board_approval_date=lambda d: pd.to_datetime(d.board_approval_date))
        .sort_values(by=["board_approval_date"], ascending=False)
    )
