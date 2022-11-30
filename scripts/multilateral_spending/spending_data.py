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
        "Education": list(range(110, 120)),
        "Health": list(range(120, 140)),
        "Water and Sanitation": list(range(140, 150)),
        "Government and Civil Society": list(range(150, 160)),
        "Other Social Infrastructure": list(range(160, 170)),
        "Transport & Storage": list(range(210, 220)),
        "Communications": list(range(220, 230)),
        "Energy": list(range(230, 240)),
        "Banking and Financial Services": list(range(240, 250)),
        "Business & Other Services": list(range(250, 260)),
        "Agriculture, Forestry and Fishing": list(range(310, 320)),
        "industry, Mining and Construction": list(range(320, 330)),
        "Trade Policy and Regulations": list(range(330, 340)),
        "General Environmental Protection": list(range(410, 430)),
        "Other Multisector": list(range(430, 440)),
        "General Budget Support": list(range(510, 520)),
        "Development Food Assistance": list(range(520, 530)),
        "Other commodity assistance": list(range(530, 540)),
        "Action Relating to Debt": list(range(600, 700)),
        "Emergency Response": list(range(700, 800)),
        "Administrative Costs of Donors": list(range(910, 930)),
        "Refugees in Donor Countries": list(range(930, 940)),
        "Unallocated/Unspecified": list(range(998, 1000)),
    }

    summary_dict = {}

    for sector, values in d.items():
        summary_dict.update({val: sector for val in values})

    return summary_dict


def _multi_donor_query(donors_dict: dict[str, tuple[str, list[int]]]) -> str:
    """Return a query string to filter a DataFrame for multilateral donors."""
    query = ""
    for bank, (name, codes) in donors_dict.items():
        query += f"(donor_code == {bank} & agency_code.isin({codes})) | "
    return query[:-3]


def _summarise_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame summarising MDB data."""

    group = [
        "year",
        "donor_code",
        "region_name",
        "recipient_code",
        "flow_name",
        "sector_code",
    ]
    cols = ["usd_commitment", "usd_disbursement"]
    return (
        data.groupby(group, observed=True)[cols]
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

    return (
        data.assign(donor_name=lambda d: d.donor_code.map(donors_dict).str[0])
    ).pipe(add_name, "recipient_code")


def _summarise_sectors(data: pd.DataFrame) -> pd.DataFrame:
    group = [
        "year",
        "donor_name",
        "region_name",
        "recipient_name",
        "flow_name",
        "sector_name",
    ]
    cols = ["usd_commitment", "usd_disbursement"]

    data = data.assign(sector_name=lambda d: d.sector_code.map(sector_groups()))

    return data.groupby(group, observed=True)[cols].sum().reset_index()


def _keep_disbursements_only(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with only disbursements."""
    return (
        data.drop("usd_commitment", axis=1)
        .loc[lambda d: d.usd_disbursement > 0]
        .reset_index(drop=True)
    )


def _simplify_africa(data: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with Africa renamed to 'Africa'."""
    return data.assign(
        region_name=lambda d: d.region_name.str.replace(
            "Africa, regional|North of Sahara|South of Sahara",
            "Africa",
            case=False,
            regex=True,
        )
    )


def mdb_data(donors_dict: dict[str, tuple[str, list[int]]] = None) -> pd.DataFrame:
    """Return a DataFrame of MDB data."""
    if donors_dict is None:
        donors_dict = config.MULTILATERALS

    data = read_crs(years=range(YEARS["start"], YEARS["end"] + 1))
    query = _multi_donor_query(donors_dict)

    return (
        data.query(query)
        .pipe(_simplify_africa)
        .pipe(_summarise_data)
        .pipe(_add_names, donors_dict)
        .pipe(_summarise_sectors)
        .pipe(_keep_disbursements_only)
        .query("year == 2020")
        .reset_index(drop=True)
        .filter(
            [
                "year",
                "donor_name",
                "region_name",
                "recipient_name",
                "sector_name",
                "flow_name",
                "usd_disbursement",
            ],
            axis=1,
        )
    )


df = mdb_data()
