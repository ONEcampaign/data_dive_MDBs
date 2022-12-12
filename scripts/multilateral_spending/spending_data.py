import pandas as pd
from oda_data import set_data_path, ODAData
from oda_data.tools.names import add_name, read_crs_codes

from scripts import config
from scripts.config import YEARS

set_data_path(config.PATHS.raw_data)


def _sector_codes() -> dict:
    d = read_crs_codes()["sector_code"]

    return {int(k): v["name"] for k, v in d.items()}


def _flow_codes() -> dict:
    return {
        11: "ODA Grants",
        14: "Other Official Flows (non Export Credit",
        13: "ODA Loans",
        19: "Equity Investment",
    }


def _region_codes() -> dict:
    return {
        10003: "South of Sahara",
        10006: "South America",
        15006: "Regional and Unspecified",
        10009: "South & Central Asia",
        10010: "Europe",
        10012: "Oceania",
        10007: "Asia",
        10011: "Middle East",
        10008: "Far East Asia",
        10005: "Caribbean & Central America",
        10002: "North of Sahara",
        10004: "America",
        10001: "Africa",
        298: "Africa, regional",
        798: "Asia, regional",
    }


def _sector_groups():
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
        "Industry, Mining and Construction": list(range(320, 330)),
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
        "prices",
        "currency",
    ]

    data = data.assign(sector_name=lambda d: d.sector_code.map(_sector_groups()))

    return data.groupby(group, observed=True)["value"].sum().reset_index()


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


def full_mdb_data(donors_dict: dict[str, tuple[str, list[int]]] = None) -> pd.DataFrame:
    """Return a DataFrame of MDB data."""
    if donors_dict is None:
        donors_dict = config.MULTILATERALS

    years = range(YEARS["start"], YEARS["end"] + 1)

    columns = [
        "year",
        "indicator",
        "donor_code",
        "agency_code",
        "recipient_code",
        "region_code",
        "sector_code",
        "flow_code",
        "currency",
        "prices",
    ]

    oda = (
        ODAData(years=years, donors=list(donors_dict))
        .load_indicator("crs_bilateral_all_flows_disbursement_gross")
        .simplify_output_df(columns_to_keep=columns)
        .add_names()
    )

    query = _multi_donor_query(donors_dict)

    output = [
        "year",
        "donor_name",
        "region_name",
        "recipient_name",
        "flow_name",
        "sector_name",
        "prices",
        "currency",
        "value",
    ]

    return (
        oda.get_data()
        .loc[lambda d: d.value > 0]
        .query(query)
        .assign(
            flow_name=lambda d: d.flow_code.map(_flow_codes()),
            donor_name=lambda d: d.donor_code.map(donors_dict).str[0],
            region_name=lambda d: d.region_code.map(_region_codes()),
        )
        .pipe(_summarise_sectors)
        .filter(output, axis=1)
        .reset_index(drop=True)
    )
