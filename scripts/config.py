from pathlib import Path


class PATHS:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    raw_data = project / "raw_data"
    output = project / "output"
    scripts = project / "scripts"
    logs = scripts / ".logs"


MULTILATERALS: dict = {

    901: ("International Bank for Reconstruction and Development", [1]),
    905: ("International Development Association", [1]),
    906: ("Caribbean Development Bank", [x for x in range(1, 100)]),
    909: ("Inter-American Development Bank", [x for x in range(1, 100)]),
    910: ("Central American Bank for Economic Integration", [1, 2]),
    913: ("African Development Bank", [x for x in range(1, 100)]),
    914: ("African Development Fund", [x for x in range(1, 100)]),
    915: ("Asian Development Bank", [x for x in range(1, 100)]),
    918: ("European Investment Bank", [3]),
    921: ("Arab Fund", [x for x in range(1, 100)]),
    953: ("Arab Bank for Economic Development in Africa", [x for x in range(1, 100)]),
    976: ("Islamic Development Bank", [1]),
    990: ("European Bank for Reconstruction and Development", [1]),
    1013: ("Council of Europe Development Bank", [x for x in range(1, 100)]),
    1015: ("Development Bank of Latin America", [x for x in range(1, 100)]),
    1019: ("IDB Invest", [x for x in range(1, 100)]),
    1024: ("Asian Infrastructure Investment Bank", [1]),
    1037: ("International Investment Bank", [x for x in range(1, 100)]),
    1044: ("New Development Bank", [1, 2]),
}

YEARS: dict = {"start": 2015, "end": 2020}

TRACKER_URL: str = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRcvkAHbsvjJamczcVlkx-a0D1JkQIqz3jZ84ULO0FOdxp5-"
    "N1SoYMTwGEBT1Fduc_em6dk-2ImMpam/pub?gid=0&single=true&output=csv"
)