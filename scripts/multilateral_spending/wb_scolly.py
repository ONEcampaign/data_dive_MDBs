"""This script contains functions to generate the data for the World Bank scrollytelling story."""
from scripts import config
from scripts.multilateral_spending.spending_data import full_mdb_data
import pandas as pd


def _filter_years(data: pd.DataFrame, years: list[int] | int) -> pd.DataFrame:
    """Return a DataFrame with only the specified years."""
    if isinstance(years, int):
        years = [years]
    return data.loc[lambda d: d.year.isin(years)].reset_index(drop=True)


def _filter_donors(data: pd.DataFrame, donors: list[str] | str) -> pd.DataFrame:
    """Return a DataFrame with only the specified donors."""
    if isinstance(donors, str):
        donors = [donors]
    return data.loc[lambda d: d.donor_name.isin(donors)].reset_index(drop=True)


def _add_share_column(data: pd.DataFrame, group_by: list) -> pd.DataFrame:
    """Return a DataFrame with a share column, calculated by group."""

    data["share"] = data.groupby(group_by, observed=True, group_keys=False)[
        "usd_disbursement"
    ].apply(lambda x: round(100 * x / x.sum(), 3))

    return data.reset_index(drop=False)


def _overall_wb_data(years: int = 2020) -> pd.DataFrame:
    """Produce a version of the dataset for the World Bank Group"""
    if isinstance(years, int):
        years = [years]

    # World Bank agencies
    world_bank = [
        "International Bank for Reconstruction and Development",
        "International Development Association",
    ]

    # grouper
    grouper = [
        "year",
        "donor",
        "flow_name",
        "region_name",
        "recipient_name",
        "sector_name",
    ]

    return (
        full_mdb_data()
        .pipe(_filter_donors, world_bank)
        .pipe(_filter_years, 2020)
        .assign(donor="World Bank Group")
        .groupby(grouper)
        .sum(numeric_only=True)
        .reset_index(drop=False)
    )


def __df_summary(df: pd.DataFrame, grouper: list[str]) -> pd.DataFrame:
    """Helper function to summarise the data according to a certain grouper. This
    function assumes only 1 year is included"""

    return (
        df.groupby(grouper)
        .sum(numeric_only=True)
        .round(2)
        .drop(columns=["year"])
        .reset_index(drop=False)
    )


def _flow_region_recipient_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Takes the full dataset and produces a summary by flow, region and recipient
    This script assumes only 1 year of data is passed to it."""

    grouper = ["donor", "flow_name", "region_name", "recipient_name"]

    return __df_summary(df, grouper)


def _sector_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Takes the full dataset and produces a summary by sector
    This script assumes only 1 year of data is passed to it."""

    grouper = ["donor", "sector_name"]

    return __df_summary(df, grouper)


def world_bank_scrolly() -> None:
    """Produce the data for the World Bank scrollytelling story."""

    # get the full dataset for the World Bank
    overall_data = _overall_wb_data(years=2020)

    # Sector summary
    sector_summary = _sector_summary(overall_data)

    # flow, region, recipient summary
    full_summary = _flow_region_recipient_summary(overall_data)

    # save files
    sector_summary.to_csv(config.PATHS.output / "wb_sector_summary.csv", index=False)
    full_summary.to_csv(config.PATHS.output / "wb_full_summary.csv", index=False)
