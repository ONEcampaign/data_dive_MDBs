from pathlib import Path


class PATHS:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    raw_data = project / "raw_data"
    output = project / "output"
    scripts = project / "scripts"
    logs = scripts / ".logs"


TRACKER_URL: str = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRcvkAHbsvjJamczcVlkx-a0D1JkQIqz3jZ84ULO0FOdxp5-"
    "N1SoYMTwGEBT1Fduc_em6dk-2ImMpam/pub?gid=0&single=true&output=csv"
)
