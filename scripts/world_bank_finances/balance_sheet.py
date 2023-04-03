from functools import partial

import pandas as pd

from scripts import config
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
    "deferred_amounts": "Deferred amounts to maintain value of currency holdings",
    "receivable_amounts": "Receivable amounts to maintain value of currency holdings",
    "demand_obligations": "Non-negotiable, non-interest-bearing demand obligations on account of subscribed capital",
    "mov_payable": "Payable to maintain value of currency holdings on account of subscribed capital",
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


def _indicator_summary(indicators: list, indicator_name: str) -> pd.DataFrame:
    data = read_raw_data(file_name)

    dfs = []

    for indicator in indicators:
        dfs.append(data.pipe(get_indicator, indicator=indicator))

    return (
        pd.concat(dfs, ignore_index=True)
        .assign(indicator=indicator_name)
        .groupby(["indicator", "year"], as_index=False, dropna=False)
        .sum(numeric_only=True)
    )


def get_subscribed_capital() -> pd.DataFrame:
    indicators = [
        # "subscribed_capital",
        "uncalled_capital",
        "paid_in_capital",
        "special_reserve",
        "general_reserve",
        "cumulative_fair_value_adjustments",
    ]

    return _indicator_summary(indicators, "total_capital")


get_paid_in_capital = partial(
    _indicator_summary, ["paid_in_capital"], "paid_in_capital"
)


def get_usable_paid_in_capital() -> pd.DataFrame:
    indicators = [
        "paid_in_capital",
        "deferred_amounts",
        "receivable_amounts",
        "demand_obligations",
        "mov_payable",
    ]

    return _indicator_summary(indicators, "usable_paid_in_capital")


def get_usable_equity() -> pd.DataFrame:
    indicators = [
        # Usable paid-in capital
        "paid_in_capital",
        "deferred_amounts",
        "receivable_amounts",
        "demand_obligations",
        "mov_payable",
        # Reserves
        "special_reserve",
        "general_reserve",
        # Adjustments
        "cumulative_fair_value_adjustments",
    ]

    return _indicator_summary(indicators, "usable_equity")


get_loans_outstanding = partial(
    _indicator_summary, ["total_loans_outstanding"], "loans_exposure"
)


def gearing_ratio() -> pd.DataFrame:
    df = get_subscribed_capital()
    df2 = get_loans_outstanding()

    return (
        pd.concat([df, df2], ignore_index=True)
        .pivot(index="year", columns="indicator", values="amount")
        .assign(ratio=lambda d: round(100 * d.loans_exposure / d.total_capital, 2))
        .reset_index()
        .query("year.dt.year >= 1960")
        .sort_values("year", ascending=False)
        .reset_index(drop=True)
    )


def el_ratio() -> pd.DataFrame:
    df = get_usable_equity()
    df2 = get_loans_outstanding()

    data = (
        pd.concat([df, df2], ignore_index=True)
        .pivot(index="year", columns="indicator", values="amount")
        .reset_index()
    )
    # manual correction of latest data
    data.loc[lambda d: d.year == "2022-06-30", "usable_equity"] = 50_481

    return (
        data.assign(ratio=lambda d: round(100 * d.usable_equity / d.loans_exposure, 1))
        .query("year.dt.year >= 1960")
        .sort_values("year", ascending=False)
        .reset_index(drop=True)
    )


def tool_export_ratios():
    e_ratio = el_ratio()
    g_ratio = gearing_ratio()

    df = (
        e_ratio.merge(g_ratio, on="year", suffixes=("_el", "_gearing"))
        .assign(year=lambda d: d.year.dt.year)
        .query("year>=1980")
        .filter(["year", "ratio_el", "ratio_gearing"], axis=1)
        .rename(columns={"ratio_el": "el_ratio", "ratio_gearing": "gearing_ratio"})
    )

    df.to_csv(config.PATHS.output / "tool" / "ratios.csv", index=False)


if __name__ == "__main__":
    tool_export_ratios()
