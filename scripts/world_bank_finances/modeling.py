from scripts.world_bank_finances.balance_sheet import el_ratio
from scripts.world_bank_finances.lending_capacity import yearly_snapshot

df = (
    yearly_snapshot()
    .assign(year=lambda d: d.end_of_period.dt.year)
    .drop(columns=["end_of_period"])
    .set_index("year")
    .mul(1e3)
    .reset_index()
    .filter(
        ["year", "disbursed_amount", "repaid_to_ibrd"],
        axis=1,
    )
)

ratio = (
    el_ratio()
    .assign(year=lambda d: d.year.dt.year)
    .filter(["year", "loans_exposure", "usable_equity", "ratio"], axis=1)
    .assign(ratio=lambda d: d.ratio / 100)
)


data = df.merge(ratio, on="year", how="left").dropna()


from sklearn.linear_model import Ridge

# Define the independent variables
X = data[
    [
        "ratio",
        "usable_equity",
        "repaid_to_ibrd",
    ]
]

# Define the dependent variable (lending amount)
y = data["disbursed_amount"]

# Create a Ridge Regression object and fit the model to the data
model = Ridge(alpha=0.5).fit(X, y)

# Print the coefficients of the model
print("Coefficients: ", model.coef_)


import matplotlib.pyplot as plt

# Predict the lending amount using the trained model
y_pred = model.predict(X)

# Create a residual plot
residuals = y - y_pred
fig = plt.scatter(y_pred, residuals)
plt.xlabel("Predicted values")
plt.ylabel("Residuals")
plt.title("Residual plot")
plt.show()
