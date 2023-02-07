from oda_data import ODAData, read_data, set_data_path
from scripts.config import PATHS

set_data_path(PATHS.raw_data)

oda = ODAData(
    years=range(2018, 2022), donors=[4], currency="EUR", include_names=True
).load_indicator("multisystem_multilateral_contributions_disbursement_gross")

df = oda.get_data()

df = df.groupby(
    ["year", "donor_name", "channel_name"], as_index=False, dropna=False, observed=True
)[["value"]].sum(numeric_only=True)
