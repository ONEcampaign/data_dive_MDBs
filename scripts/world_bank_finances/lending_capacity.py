from scripts.world_bank_finances.loans_data import file_name, read_raw_data

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
