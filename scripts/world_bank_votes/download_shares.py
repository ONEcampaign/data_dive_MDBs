import pandas as pd
from bblocks.dataframe_tools.add import add_population_column, add_income_level_column
from bblocks.cleaning_tools.clean import convert_id
from scripts import config, common
from scripts.logger import logger
from bblocks import set_bblocks_data_path

set_bblocks_data_path(config.PATHS.raw_data)

from bblocks import set_bblocks_data_path

set_bblocks_data_path(config.PATHS.raw_data)

URLs = {
    "IBRD": "https://finances.worldbank.org/resource/rcx4-r7xj.csv",
    "IDA": "https://finances.worldbank.org/resource/v84d-dq44.csv",
}


def download_raw_data(dataset: str) -> None:
    """Download the raw data from the WB"""
    url = URLs[dataset]
    pd.read_csv(url, parse_dates=["as_of_date"]).to_feather(
        config.PATHS.raw_data / f"{dataset}_data.feather"
    )

    logger.info(f"Downloaded {dataset} votes data")


def read_raw_data(dataset: str) -> pd.DataFrame:
    """Read the raw data from the WB"""
    return pd.read_feather(config.PATHS.raw_data / f"{dataset}_data.feather")


def extract_as_of_date(df: pd.DataFrame) -> str:
    """Extract the latest date of the data"""
    return df.as_of_date.max().strftime("%d %B %Y")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw data from the WB"""

    df = df.rename(columns={"member": "member_country"})

    return (
        df.filter(["member_country", "percentage_of_total_votes"])
        .assign(name=lambda d: convert_id(d.member_country, to_type="short_name"))
        .pipe(
            add_population_column,
            id_column="name",
            id_type="regex",
        )
        .pipe(
            add_income_level_column,
            id_column="name",
            id_type="regex",
        )
        .filter(["name", "percentage_of_total_votes", "population", "income_level"])
        .rename(columns={"percentage_of_total_votes": "Votes Share"})
        .sort_values("Votes Share", ascending=False)
        .reset_index(drop=True)
    )


def ibrd_subscriptions() -> pd.DataFrame:
    df = read_raw_data("IBRD")


def update_votes_data() -> None:
    for dataset in URLs:
        download_raw_data(dataset)


def _rename_other_income(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(
        {
            "name": {
                "High income": "Other high income",
                "Upper middle income": "Other upper middle income",
                "Lower middle income": "Other lower middle income",
                "Low income": "Low income",
            }
        }
    )


def _sort_by_income(df: pd.DataFrame, dataset: str = "IBRD") -> pd.DataFrame:
    income_sorting = {
        "High income": 1,
        "Upper middle income": 2 if dataset == "IBRD" else 3,
        "Lower middle income": 3 if dataset == "IBRD" else 2,
        "Low income": 4,
    }
    return (
        df.assign(income_sorting=lambda d: d.income_level.map(income_sorting))
        .sort_values(
            ["dataset", "income_sorting", "Votes Share"], ascending=(True, True, False)
        )
        .drop(columns=["income_sorting"])
        .reset_index(drop=True)
    )


def _top_and_groups(
    df: pd.DataFrame, top: int = 9, highlight: list = None
) -> pd.DataFrame:
    """From the clean data, get the top X countries, plus China,
    and India, and group any remaining countries by income level"""

    # If no countries to highlight are provided, use China and India
    if highlight is None:
        highlight = ["China", "India"]

    # Find the top X countries
    top_members = df.nlargest(top, "Votes Share").name.to_list()

    # Create a dataframe with the top X countries
    top_df = df[df.name.isin(top_members)]

    # Create a dataframe with the remaining countries
    not_top_df = df[~df.name.isin(top_members)]

    # From the remaining countries, get highlight countries
    highlight_countries = not_top_df.query(f"name in {highlight}")

    # Create a dataframe with the remaining countries.
    not_data = not_top_df.query(f"name not in {highlight}")

    # From the remaining countries, group by income level
    not_data = (
        not_data.groupby(["income_level", "dataset"])
        .sum(numeric_only=True)
        .reset_index()
    )

    # Combine all data, and rename income levels
    return (
        pd.concat([top_df, highlight_countries, not_data], ignore_index=True)
        .assign(name=lambda d: d["name"].fillna(d["income_level"]))
        .pipe(_rename_other_income)
    )


def votes_chart_data():
    """Prepare the data for the votes chart"""

    # Get clean versions of the raw data
    ida = (
        read_raw_data("IDA")
        .pipe(clean_data)
        .assign(dataset="International Development Association")
    )
    ibrd = (
        read_raw_data("IBRD")
        .pipe(clean_data)
        .assign(dataset="International Bank for Reconstruction and Development")
    )

    ida_grouped = _top_and_groups(ida, top=9, highlight=["China", "India"]).pipe(
        _sort_by_income, dataset="IDA"
    )
    ibrd_grouped = _top_and_groups(ibrd, top=9, highlight=["China", "India"]).pipe(
        _sort_by_income, dataset="IBRD"
    )

    # Extract group counts
    ida_totals = ida.groupby("income_level").size().to_dict()
    ida_totals = {k: f"{k} ({v} countries)" for k, v in ida_totals.items()}
    ibrd_totals = ibrd.groupby("income_level").size().to_dict()
    ibrd_totals = {k: f"{k} ({v} countries)" for k, v in ibrd_totals.items()}

    ida_grouped = ida_grouped.assign(
        income_level=lambda d: d.income_level.map(ida_totals)
    )

    ibrd_grouped = ibrd_grouped.assign(
        income_level=lambda d: d.income_level.map(ibrd_totals)
    )

    # Combine the data
    data = pd.concat([ibrd_grouped, ida_grouped], ignore_index=True)

    # Save the data
    data.to_csv(config.PATHS.output / "wb_votes_data.csv", index=False)

    # Extract dates
    ida_date = read_raw_data("IDA").pipe(extract_as_of_date)
    ibrd_date = read_raw_data("IBRD").pipe(extract_as_of_date)

    key_numbers = {"ida_date": ida_date, "ibrd_date": ibrd_date}

    common.update_key_number(
        path=config.PATHS.output / "world_bank_key_numbers.json", new_dict=key_numbers
    )
