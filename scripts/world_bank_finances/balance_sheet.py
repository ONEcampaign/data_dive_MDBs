from functools import partial

import pandas as pd
from bblocks.dataframe_tools.add import add_population_column, add_income_level_column
from bblocks.cleaning_tools.clean import convert_id
from scripts import config, common
from scripts.logger import logger
from scripts.world_bank_finances import download

file_name: str = f"IBRD_historical_balance_sheet"

INDICATORS = {
    "total_assets": "Total Assets",
    "total_loans": "Total Loans",
    "total_loans_undisbursed": "Less: Undisbursed balance of effective loans",
    "total_loans_outstanding": "Loans Outstanding",
    "total_loans_outstanding_net": "Net Loans Outstanding",
    "subscribed_capital": "Subscribed Capital",
    "paid_in_capital": "Paid-in capital",
    "uncalled_capital": "Less: uncalled portion of subscriptions",
    "general_reserve": "General Reserve",
    "special_reserve": "Special Reserve",
    "cumulative_fair_value_adjustments": "Cumulative Fair Value Adjustments",
    "borrowings": "Borrowings",
}

download_financial_data = partial(
    download.download_data, file="xnse-jvg2", file_name=file_name
)


def read_raw_data(dataset: str) -> pd.DataFrame:
    """Read the raw data from the WB"""
    return pd.read_feather(config.PATHS.raw_data / f"{dataset}_data.feather").assign(
        year=lambda d: pd.to_datetime(d.year, infer_datetime_format=True),
        amount=lambda d: pd.to_numeric(d.amount),
    )


def get_indicator(data: pd.DataFrame, indicator: str) -> pd.DataFrame:
    indicators = {k.lower(): v.lower() for k, v in INDICATORS.items()}
    indicator = indicators[indicator.lower()]

    return (
        data.loc[lambda d: d.category.str.lower() == indicator]
        .sort_values(["year"], ascending=False)
        .reset_index(drop=True)
    )


def get_usable_equity() -> pd.DataFrame:
    data = read_raw_data(file_name)

    indicators = [
        "paid_in_capital",
        "special_reserve",
        "general_reserve",
        "cumulative_fair_value_adjustments",
    ]

    dfs = []

    for indicator in indicators:
        dfs.append(data.pipe(get_indicator, indicator=indicator))

    return (
        pd.concat(dfs, ignore_index=True)
        .assign(indicator="usable_equity")
        .groupby(["indicator", "year"], as_index=False, dropna=False)
        .sum(numeric_only=True)
    )


def get_loans_exposure() -> pd.DataFrame:
    indicators = ["total_loans_outstanding"]

    dfs = []

    data = read_raw_data(file_name)

    for indicator in indicators:
        dfs.append(data.pipe(get_indicator, indicator=indicator))

    return (
        pd.concat(dfs, ignore_index=True)
        .assign(indicator="loans_exposure")
        .groupby(["indicator", "year"], as_index=False, dropna=False)
        .sum(numeric_only=True)
    )


def el_ratio() -> pd.DataFrame:
    df = get_usable_equity()
    df2 = get_loans_exposure()

    return (
        pd.concat([df, df2], ignore_index=True)
        .pivot(index="year", columns="indicator", values="amount")
        .assign(ratio=lambda d: round(100 * d.usable_equity / d.loans_exposure, 1))
        .reset_index()
        .query("year.dt.year >= 1960")
        .sort_values("year", ascending=False)
        .reset_index(drop=True)
    )


if __name__ == "__main__":

    ratio = el_ratio()
