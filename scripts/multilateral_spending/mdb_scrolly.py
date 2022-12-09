from scripts.multilateral_spending.spending_data import full_mdb_data
import pandas as pd


def _filter_years(data: pd.DataFrame, years: list[int] | int) -> pd.DataFrame:
    """Return a DataFrame with only the specified years."""
    if isinstance(years, int):
        years = [years]
    return data.loc[lambda d: d.year.isin(years)].reset_index(drop=True)


def _filter_donors(data: pd.DataFrame, donors: list[str] | str) -> pd.DataFrame:
    """Return a DataFrame with only the specified donors."""
    if isinstance(donors, int):
        donors = [donors]
    return data.loc[lambda d: d.donor_name.isin(donors)].reset_index(drop=True)


def _activities_by_donor(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with the number of activities per donor per year."""
    return data.groupby(["year", "donor", "donor_name"], observed=True).sum(
        numeric_only=True
    )


def _activities_by_donor_region(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with the number of activities per donor per year."""
    return data.groupby(
        ["year", "donor", "donor_name", "region_name"], observed=True
    ).sum(numeric_only=True)


def _activities_by_donor_sector(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with the number of activities per donor per year."""
    return data.groupby(
        ["year", "donor", "donor_name", "sector_name"], observed=True
    ).sum(numeric_only=True)


def _activities_by_donor_flow(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with the number of activities per donor per year."""
    return data.groupby(
        ["year", "donor", "donor_name", "flow_name"], observed=True
    ).sum(numeric_only=True)


def _activities_by_donor_flow_name_region_name_sector_name(
    data: pd.DataFrame,
) -> pd.DataFrame:
    """Return a DataFrame with the number of activities per donor per year."""
    return data.groupby(
        ["year", "donor", "flow_name", "region_name", "sector_name"], observed=True
    ).sum(numeric_only=True)


def _add_share_column(data: pd.DataFrame, group_by: list) -> pd.DataFrame:
    """Return a DataFrame with a share column, calculated by group."""

    data["share"] = data.groupby(group_by, observed=True, group_keys=False)[
        "usd_disbursement"
    ].apply(lambda x: round(100 * x / x.sum(), 3))

    return data.reset_index(drop=False)


def _rename_flows(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with the number of activities per donor per year."""
    return data.assign(
        flow_name=lambda d: d.flow_name.replace(
            {"Other Official Flows (non Export Credit)": "Loans (Other Official Flows)"}
        )
    )


ds = [
    "International Bank for Reconstruction and Development",
    "International Development Association",
]

data = (
    full_mdb_data()
    .pipe(_filter_years, 2020)
    .pipe(_filter_donors, ds)
    .assign(donor="World Bank Group")
)

total = _activities_by_donor(data)
total_region = _activities_by_donor_region(data)
total_sector = _activities_by_donor_sector(data)
total_flow = _activities_by_donor_flow(data)

journey = _activities_by_donor_flow_name_region_name_sector_name(data)

journey2 = (
    _add_share_column(journey, ["year", "donor"])
    .pipe(_rename_flows)
    .filter(
        [
            "donor",
            "flow_name",
            "region_name",
            "sector_name",
            "usd_disbursement",
            "share",
        ],
        axis=1,
    )
)

journey2.to_clipboard(index=False)
