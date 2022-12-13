import pandas as pd
from bblocks.dataframe_tools.add import add_population_column, add_income_level_column
from bblocks.cleaning_tools.clean import convert_id
from scripts import config

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


def read_raw_data(dataset: str) -> pd.DataFrame:
    """Read the raw data from the WB"""
    return pd.read_feather(config.PATHS.raw_data / f"{dataset}_data.feather")


def extract_as_of_date(df: pd.DataFrame) -> str:
    return df.as_of_date.max().strftime("%d %B %Y")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    df = df.rename(columns={"member": "member_country"})

    return (
        df.filter(["member_country", "percentage_of_total_votes"])
        .assign(name=lambda d: convert_id(d.member_country, to_type="short_name"))
        .pipe(
            add_population_column,
            id_column="name",
            id_type="regex",
            data_path=f"{config.PATHS.raw_data}",
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


def chart():

    ida = read_raw_data("IDA").pipe(clean_data).assign(dataset="IDA")
    ibrd = read_raw_data("IBRD").pipe(clean_data).assign(dataset="IBRD")

    top_ida = ida.nlargest(5, "Votes Share").name.to_list()
    top_ibrd = ibrd.nlargest(5, "Votes Share").name.to_list()

    top_ida = ida.query(f"name in {top_ida}")
    not_top_ida = ida.query(f"name not in {top_ida.name.to_list()}")

    top_ibrd = ibrd.query(f"name in {top_ibrd}")
    not_top_ibrd = ibrd.query(f"name not in {top_ibrd.name.to_list()}")

    not_data = pd.concat([not_top_ida, not_top_ibrd], ignore_index=True)

    china_india = not_data.query("name in ['China','India']")

    not_data = not_data.query("name not in ['China','India']")

    not_data = (
        not_data.groupby(["income_level", "dataset"])
        .sum(numeric_only=True)
        .reset_index()
    )

    data = pd.concat([top_ida, top_ibrd, china_india, not_data], ignore_index=True)
    data.name = data.name.fillna(data.income_level)

    data = data.replace(
        {
            "name": {
                "High income": "Other high income",
                "Upper middle income": "Other upper middle income",
                "Lower middle income": "Other lower middle income",
                "Low income": "Low income",
            }
        }
    )
    income_sorting = {
        "High income": 1,
        "Upper middle income": 2,
        "Lower middle income": 3,
        "Low income": 4,
    }
    data = data.assign(income_sorting=lambda d: d.income_level.map(income_sorting))

    data = data.sort_values(
        ["income_sorting", "Votes Share"], ascending=(True, False)
    ).drop(columns=["income_sorting"])

    data = data.replace(
        {
            "IDA": "International Development Association",
            "IBRD": "International Bank for Reconstruction and Development",
        }
    )

    data.to_clipboard(index=False)
