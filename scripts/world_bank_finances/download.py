import pandas as pd
from sodapy import Socrata

from scripts import config
from scripts.logger import logger


def download_data(file: str, file_name: str) -> None:
    client = Socrata("finances.worldbank.org", None)
    results = client.get(file, limit=2_000_000)

    dtypes = {
        "end_of_period": "datetime64[ns]",
        "loan_number": "category",
        "region": "category",
        "country_code": "category",
        "country": "category",
        "borrower": "category",
        "guarantor_country_code": "category",
        "guarantor": "category",
        "loan_type": "category",
        "loan_status": "category",
        "interest_rate": "float64",
        "project_id": "category",
        "project_name_": "category",
        "original_principal_amount": "float64",
        "cancelled_amount": "float64",
        "undisbursed_amount": "float64",
        "disbursed_amount": "float64",
        "repaid_to_ibrd": "float64",
        "due_to_ibrd": "float64",
        "exchange_adjustment": "float64",
        "borrower_s_obligation": "float64",
        "sold_3rd_party": "float64",
        "repaid_3rd_party": "float64",
        "due_3rd_party": "float64",
        "loans_held": "float64",
        "first_repayment_date": "datetime64[ns]",
        "last_repayment_date": "datetime64[ns]",
        "agreement_signing_date": "datetime64[ns]",
        "board_approval_date": "datetime64[ns]",
        "effective_date_most_recent_": "datetime64[ns]",
        "closed_date_most_recent_": "datetime64[ns]",
        "last_disbursement_date": "datetime64[ns]",
    }
    df = pd.DataFrame.from_records(results)

    dtypes = {k: v for k, v in dtypes.items() if k in df.columns}

    df.astype(dtypes).to_feather(config.PATHS.raw_data / f"{file_name}_data.feather")

    logger.info(f"Downloaded {file_name}_data")
