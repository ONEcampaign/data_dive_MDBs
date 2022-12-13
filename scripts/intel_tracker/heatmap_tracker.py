"""This script fetches data from a google sheet used by the ONE team to track
positions on the recommendations. It then reshapes the data into a format
suitable for plotting a Flourish heatmap."""

import pandas as pd

from scripts import config

# mapping of the recommendations used by the tracking sheet and the user-friendly names
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


def _clean_columns(col: str) -> str:
    """Clean the column names."""
    return col.lower().strip().replace(" ", "_")


def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw data from the tracker."""
    return (
        df.rename(columns=lambda d: _clean_columns(d))
        .dropna(subset=["country"])
        .reset_index(drop=True)
    )


def _extract_support(df: pd.DataFrame) -> pd.DataFrame:
    """Extract the support data from the tracker."""
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


def _reshape_support(df: pd.DataFrame) -> pd.DataFrame:
    return df.melt(id_vars="country", var_name="question", value_name="support").fillna(
        "No data"
    )


def _rename_support(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "support": "Support",
            "country": "Country",
            "question": "Recommendation",
        }
    ).replace(RECS, regex=False)


def heatmap_data() -> None:
    """Extract the support data from the tracker and reshape it for plotting."""

    # Get the data nd run it through the pipeline
    data = (
        download_raw_data()
        .pipe(_clean_data)
        .pipe(_extract_support)
        .pipe(_reshape_support)
        .pipe(_rename_support)
    )

    data.to_csv(config.PATHS.output / "heatmap_data.csv", index=False)


if __name__ == "__main__":
    heatmap_data()
