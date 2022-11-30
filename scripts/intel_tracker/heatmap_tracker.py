import pandas as pd

from scripts import config


RECS: dict = {
    "overall_support": "Overall support",
    "recommendation_1": "Adapt approach to defining risk tolerance",
    "recommendation_2": "Give more credit to callable capital",
    "recommendation_3": "Expand uses of financial innovations",
    "recommendation_4": "Improve credit rating agency assessments",
    "recommendation_5": "Increase access to MDB data and analysis",
}


def download_raw_data(url: str = config.TRACKER_URL) -> pd.DataFrame:
    """Download the raw data from the tracker."""
    return pd.read_csv(url, encoding="utf-8")


def clean_columns(col: str) -> str:
    return col.lower().strip().replace(" ", "_")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.rename(columns=lambda d: clean_columns(d))
        .dropna(subset=["country"])
        .reset_index(drop=True)
    )


def extract_support(df: pd.DataFrame) -> pd.DataFrame:
    return df.filter(
        [
            "country",
            "overall_support",
            "recommendation_1",
            "recommendation_2",
            "recommendation_3",
            "recommendation_4",
            "recommendation_5",
        ],
        axis=1,
    )


def reshape_support(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(id_vars="country", var_name="question", value_name="support").fillna(
        "No data"
    )


def rename_support(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "support": "Support",
            "country": "Country",
            "question": "Recommendation",
        }
    ).replace(RECS, regex=False)


def heatmap_data() -> None:

    data = (
        download_raw_data()
        .pipe(clean_data)
        .pipe(extract_support)
        .pipe(reshape_support)
        .pipe(rename_support)
    )

    data.to_csv(config.PATHS.output / "heatmap_data.csv", index=False)


if __name__ == "__main__":
    heatmap_data()
