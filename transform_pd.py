import numpy as np
import pandas as pd
import re

from constants import *


def cleanup_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    new_df_clean = df.replace(to_replace=NA_VALUES, value=np.nan)

    new_df_clean = (
        new_df_clean
        .assign(
            **{
                var: new_df_clean[var].str.replace(pat="_", repl="")
                for var in VARS_WITH_TRAILING_UNDERSCORES
            }
        )
        .assign(
            Type_of_Loan=new_df_clean["Type_of_Loan"].apply(_parse_type_of_loan),
            Credit_History_Age=new_df_clean["Credit_History_Age"].apply(_parse_credit_history_age),
            Payment_of_Min_Amount=new_df_clean["Payment_of_Min_Amount"].apply(_parse_payment_of_min_amount)
        )
        .astype({var: float for var in VARS_TO_CAST})
    )

    return new_df_clean


def replace_outliers(df: pd.DataFrame) -> pd.DataFrame:
    per_group_medians = df[VARS_WITH_OUTLIERS_TOLERANCES.keys()].groupby(df["Customer_ID"]).transform("median")
    output_df = df.assign(
        **{
            var: df[var].mask((df[var] - per_group_medians[var]).abs() > tolerance, other=np.nan)
            for var, tolerance in VARS_WITH_OUTLIERS_TOLERANCES.items()
        }
    )

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

    output_df[VARS_REPEAT] = repeat[VARS_REPEAT].values
    output_df[VARS_INTERPOLATE] = interpolate[VARS_INTERPOLATE].values

    return output_df


def prepare_training_data(df: pd.DataFrame):
    df = cleanup_and_cast(df)
    df = replace_outliers(df)
    df = fill_nas_from_other_customer_records(df)

    return df


def prepare_test_data(
    test_df: pd.DataFrame,
    reference_df: pd.DataFrame
):
    test_months = test_df.Month.unique().tolist()
    test_df = cleanup_and_cast(test_df)

    output_df = reference_df.drop(["Credit_Score"], axis=1)
    for month in test_months:
        output_df = pd.concat([output_df, test_df[test_df.Month == month]])
        output_df = replace_outliers(output_df)
        output_df = fill_nas_from_other_customer_records(output_df)

    return output_df[output_df.Month.isin(test_months)]


def _parse_type_of_loan(value: str | float) -> list[str] | float:
    return re.split(r", and |, ", value) if type(value) == str else []


def _parse_credit_history_age(value: str | float) -> float:
    if type(value) != str:
        return value
    years, months = re.findall(r"\d+", value)
    return float(years) + float(months) / 12


def _parse_payment_of_min_amount(value: str | float):
    return value == "Yes" if type(value) == str else value
