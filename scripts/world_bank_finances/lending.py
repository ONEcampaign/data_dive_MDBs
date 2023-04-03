import pandas as pd

from scripts.world_bank_finances.loans_data import file_name, read_raw_data


def cumulative_lending(end_of_period: str | list[str]):
    """Calculate the cumulative lending since a starting date"""
    exclude = ["Draft", "Cancelled", "Fully Cancelled"]
    cols = [
        "country",
        "end_of_period",
        "disbursed_amount",
        "cumulative_lending",
        "repaid_to_ibrd",
        "due_to_ibrd",
        "exchange_adjustment",
        "undisbursed_amount",
        "borrower_s_obligation",
    ]

    if isinstance(end_of_period, str):
        end_of_period = [end_of_period]

    df = (
        read_raw_data(file_name)
        .query(f"loan_status not in {exclude}")
        .loc[lambda d: d.end_of_period.isin(end_of_period)]
        .filter(cols, axis=1)
        .groupby(["country", "end_of_period"], observed=True, dropna=False)
        .sum(numeric_only=True)
        .div(1e9)
        .reset_index()
        .sort_values(["due_to_ibrd"], ascending=False)
    )

    return df


def loans_outstanding_ts() -> pd.DataFrame:
    dates = [f"{y}-06-30" for y in range(2011, 2024)] + ["2022-12-31"]

    df = (
        cumulative_lending(dates)
        .groupby(["end_of_period"], observed=True, dropna=False)
        .sum(numeric_only=True)
        .reset_index()
        .assign(outstanding=lambda d: d.due_to_ibrd + d.exchange_adjustment)
        .filter(["disbursed_amount", "outstanding", "end_of_period"], axis=1)
        .melt(id_vars=["end_of_period"])
        .replace(
            {
                "disbursed_amount": "Cumulative historical disbursements",
                "outstanding": "Cumulative outstanding loans",
            }
        )
    )
    df.to_clipboard(index=False)

    return df


exclude = [
    "Draft",
    "Cancelled",
    "Fully Cancelled",
    "Fully Transferred",
    "Terminated",
    "Approved",
    "Signed",
]
cols = [
    "country",
    "loan_status",
    "end_of_period",
    "disbursed_amount",
    "cumulative_lending",
    "repaid_to_ibrd",
    "due_to_ibrd",
    "exchange_adjustment",
    "undisbursed_amount",
    "borrower_s_obligation",
]

dates = [f"{y}-06-30" for y in range(2011, 2024)]


def latest_snapshot():
    return (
        read_raw_data(file_name)
        .filter(cols, axis=1)
        .query(f"loan_status not in {exclude}")
        .query("end_of_period in @dates")
        .groupby(["end_of_period"], observed=True, dropna=False)
        .sum(numeric_only=True)
        .div(1e9)
        .reset_index()
    )


def yearly_snapshot():
    return (
        read_raw_data(file_name)
        .filter(cols, axis=1)
        .query(f"loan_status not in {exclude}")
        .query("end_of_period in @dates")
        .groupby(["end_of_period"], observed=True, dropna=False)
        .sum(numeric_only=True)
        .div(1e9)
        .diff(1)
        .reset_index()
    )


if __name__ == "__main__":
    ...
    df = cumulative_lending("2022-06-30")
