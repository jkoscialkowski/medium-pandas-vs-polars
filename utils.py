import numpy as np
import pandas as pd
import re

from constants import *


def get_non_castable(df: pd.DataFrame, colname: str, desired_dtype: type):
    output = []
    for val in df[colname]:
        try:
            desired_dtype(val)
        except:
            output.append(val)

    return output


def cleanup_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    new_df_clean = df.replace(to_replace=NA_VALUES, value=np.nan)

    for var in VARS_WITH_TRAILING_UNDERSCORES:
        new_df_clean[var] = new_df_clean[var].str.replace(pat="_", repl="")

    new_df_clean.loc[:, "Type_of_Loan"] = new_df_clean["Type_of_Loan"].apply(_parse_type_of_loan)
    new_df_clean.loc[:, "Credit_History_Age"] = new_df_clean["Credit_History_Age"].apply(_parse_credit_history_age)
    new_df_clean.loc[:, "Payment_of_Min_Amount"] = new_df_clean["Payment_of_Min_Amount"].apply(_parse_payment_of_min_amount)

    new_df_clean = new_df_clean.astype({var: float for var in VARS_TO_CAST})
    return new_df_clean


def replace_outliers(df: pd.DataFrame) -> pd.DataFrame:
    output_df = df.copy(deep=True)
    for var, tolerance in VARS_WITH_OUTLIERS_TOLERANCES.items():
        per_group_medians = df[var].groupby(df["Customer_ID"]).transform("median")
        which_outliers = (df[var] - per_group_medians).abs() > tolerance
        output_df[var].mask(which_outliers, other=np.nan, inplace=True)

    return output_df


def fill_nas_from_other_customer_records(df: pd.DataFrame) -> pd.DataFrame:
    output_df = df.copy(deep=True)
    repeat = (
        df[["Customer_ID"] + VARS_REPEAT]
        .set_index("Customer_ID")
        .groupby(level=0)
        .bfill()
        .groupby(level=0)
        .ffill()
        .reset_index()
    )
    interpolate = (
        df[["Customer_ID"] + VARS_INTERPOLATE]
        .groupby("Customer_ID")
        .transform(lambda x: x.interpolate(limit_direction="both"))
    )

    output_df[VARS_REPEAT] = repeat[VARS_REPEAT]
    output_df[VARS_INTERPOLATE] = interpolate[VARS_INTERPOLATE]

    return output_df


def prepare_data(
    df: pd.DataFrame,
    reference_df: pd.DataFrame | None = None,
):
    df = cleanup_and_cast(df)

    if reference_df is not None:
        df = pd.concat([reference_df, df], ignore_index=True)

    df = replace_outliers(df)
    df = fill_nas_from_other_customer_records(df)

    if reference_df is not None:
        df = df.iloc[reference_df.shape[0]:, :]

    return df


def _parse_type_of_loan(value: str | float) -> list[str] | float:
    return re.split(r", and |, ", value) if type(value) == str else []


def _parse_credit_history_age(value: str | float) -> float:
    if type(value) != str:
        return value
    years, months = re.findall(r"\d+", value)
    return float(years) + float(months) / 12


def _parse_payment_of_min_amount(value: str | float):
    return value == "Yes" if type(value) == str else value
