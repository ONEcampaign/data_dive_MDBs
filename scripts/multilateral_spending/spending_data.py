import pandas as pd
from oda_data import set_data_path
from oda_data.read_data.read import read_crs

from oda_data.clean_data.names import add_name, read_crs_codes

from scripts import config
from scripts.config import YEARS

set_data_path(config.PATHS.raw_data)


def _sector_codes() -> dict:
    d = read_crs_codes()["SectorCategory"]

    return {int(k): v["name"] for k, v in d.items()}


def sector_groups():
    d = {
        "Education": range(110, 120),
        "Health": range(120, 140),
        "Water and Sanitation": range(140, 150),
        "Government and Civil Society": range(150, 160),
        "Other Social Infrastructure": range(160, 170),
        "Transport & Storage": range(210, 220),
        "Communications": range(220, 230),
        "Energy": range(230, 240),
        "Banking and Financial Services": range(240, 250),
        "Business & Other Services": range(250, 260),
        "Agriculture, Forestry and Fishing": range(310, 320),
        "industry, Mining and Construction": range(320, 330),
        "Trade Policy and Regulations": range(330, 340),
        "General Environmental Protection": range(410, 430),
        "Other Multisector": range(430, 440),
        "General Budget Support": range(510, 520),
        "Development Food Assistance": range(520, 530),
        "Other commodity assistance": range(530, 540),
        "Action Relating to Debt": range(600, 700),
        "Emergency Response": range(700, 800),
        "Administrative Costs of Donors": range(910, 930),
        "Refugees in Donor Countries": range(930, 940),
        "Unallocated/Unspecified": range(998, 1000),
    }

    return {v: k for k, v in d.items()}
    #return d

def _multi_donor_query(donors_dict: dict[str, tuple[str, list[int]]]) -> str:
    """Return a query string to filter a DataFrame for multilateral donors."""
    query = ""
    for bank, (name, codes) in donors_dict.items():
        query += f"(donor_code == {bank} & agency_code.isin({codes})) | "
    return query[:-3]


def _summarise_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame summarising MDB data."""

    group = ["year", "donor_code", "recipient_code", "flow_name", "sector_code"]
    cols = ["usd_commitment", "usd_disbursement"]
    return (
        data.groupby(group)[cols]
        .sum()
        .loc[lambda d: d.sum(axis=1) > 0]
        .reset_index()
        .sort_values(
            ["year", "donor_code", "usd_disbursement"], ascending=(False, True, False)
        )
        .reset_index(drop=True)
    )


def _add_names(
    data: pd.DataFrame, donors_dict: dict[str, tuple[str, list[int]]]
) -> pd.DataFrame:

    return data.assign(
        donor_name=lambda d: d.donor_code.map(donors_dict).str[0],
        sector_name=lambda d: d.sector_code.map(_sector_codes()),
    ).pipe(add_name, "recipient_code")


def mdb_data(donors_dict: dict[str, tuple[str, list[int]]] = None) -> pd.DataFrame:
    """Return a DataFrame of MDB data."""
    if donors_dict is None:
        donors_dict = config.MULTILATERALS

    data = read_crs(years=range(YEARS["start"], YEARS["end"] + 1))
    query = _multi_donor_query(donors_dict)

    data = data.query(query).pipe(_summarise_data).pipe(_add_names, donors_dict)
