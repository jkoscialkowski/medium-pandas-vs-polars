NA_VALUES = {
    "Occupation": "_______",
    "Changed_Credit_Limit": "_",
    "Credit_Mix": "_",
    "Payment_of_Min_Amount": "NM",
    "Amount_invested_monthly": "__10000__",
    "Monthly_Balance": "__-333333333333333333333333333__",
}
VARS_WITH_TRAILING_UNDERSCORES = ["Age", "Annual_Income", "Num_of_Loan", "Num_of_Delayed_Payment", "Outstanding_Debt"]
VARS_TO_CAST = [
    "Age",
    "Annual_Income",
    "Num_of_Loan",
    "Num_of_Delayed_Payment",
    "Changed_Credit_Limit",
    "Outstanding_Debt",
    "Credit_History_Age",
    "Payment_of_Min_Amount",
    "Amount_invested_monthly",
    "Monthly_Balance",
]
VARS_WITH_OUTLIERS_TOLERANCES = {
    "Age": 1.,
    "Annual_Income": 100.,
    "Num_Bank_Accounts": 1.,
    "Num_Credit_Card": 1.,
    "Interest_Rate": 1.,
    "Num_of_Loan": 1.,
    "Num_of_Delayed_Payment": 1.,
    "Num_Credit_Inquiries": 1.,
    "Total_EMI_per_month": 10.,
}
VARS_REPEAT = ["Name", "Age", "Occupation", "Credit_Mix"]
VARS_INTERPOLATE = ["Annual_Income", "Monthly_Inhand_Salary", "Num_Bank_Accounts", "Credit_History_Age"]