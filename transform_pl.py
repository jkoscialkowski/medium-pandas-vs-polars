import numpy as np
import polars as pl
import re

from constants import *


def cleanup_and_cast(df: pl.DataFrame) -> pl.DataFrame:
    new_df_clean = (
        df
        .with_columns(
            *[pl.col(var).map_dict({value: pl.Null}, default=pl.first()) for var, value in NA_VALUES.items()]
        )
        .with_columns(pl.col(*VARS_WITH_TRAILING_UNDERSCORES).str.replace(pattern="_", value=""))
        .with_columns(
            pl.col("Type_of_Loan").map_elements(_parse_type_of_loan, skip_nulls=False),
            pl.col("Credit_History_Age").map_elements(_parse_credit_history_age),
            pl.col("Payment_of_Min_Amount").map_elements(_parse_payment_of_min_amount)
        )
        .cast({var: pl.Float64 for var in VARS_TO_CAST})
    )

    return new_df_clean


def replace_outliers(df: pl.DataFrame) -> pl.DataFrame:
    per_group_medians = df.select(
        pl.col(VARS_WITH_OUTLIERS_TOLERANCES.keys())
        .median()
        .over("Customer_ID")
    )
    output_df = df.with_columns(
        *[
            pl
            .when((pl.col(var) - per_group_medians[var]).abs() <= tol)
            .then(pl.col(var))
            for var, tol in VARS_WITH_OUTLIERS_TOLERANCES.items()
        ]
    )

    return output_df


def fill_nas_from_other_customer_records(df: pl.DataFrame) -> pl.DataFrame:
    output_df = df.with_columns(
        pl.col(VARS_INTERPOLATE)
        .interpolate()
        .over("Customer_ID")
    ).with_columns(
        pl.col(VARS_REPEAT + VARS_INTERPOLATE)
        .fill_null(strategy="backward")
        .fill_null(strategy="forward")
        .over("Customer_ID")
    )

    return output_df


def prepare_test_data(
    test_df: pl.DataFrame,
    reference_df: pl.DataFrame
):
    test_months = test_df["Month"].unique().to_list()
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
