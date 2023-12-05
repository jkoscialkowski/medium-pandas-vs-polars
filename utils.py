import pandas as pd
import polars as pl

import transform_pd as tpd
import transform_pl as tpl


def get_non_castable(df: pd.DataFrame | pl.DataFrame, colname: str, desired_dtype: type):
    output = []
    for val in df[colname]:
        try:
            desired_dtype(val)
        except:
            output.append(val)

    return output


def prepare_training_data(df: pd.DataFrame | pl.DataFrame):
    transform_module = tpd if isinstance(df, pd.DataFrame) else tpl
    df = transform_module.cleanup_and_cast(df)
    df = transform_module.replace_outliers(df)
    df = transform_module.fill_nas_from_other_customer_records(df)

    return df


def prepare_test_data(
    test_df: pd.DataFrame | pl.DataFrame,
    reference_df: pd.DataFrame | pl.DataFrame
):
    transform_module = tpd if isinstance(test_df, pd.DataFrame) else tpl
    test_months = test_df["Month"].unique().tolist()
    test_df = transform_module.cleanup_and_cast(test_df)

    output_df = reference_df.drop(["Credit_Score"], axis=1)
    for month in test_months:
        output_df = pd.concat([output_df, test_df[test_df.Month == month]])
        output_df = transform_module.replace_outliers(output_df)
        output_df = transform_module.fill_nas_from_other_customer_records(output_df)

    return output_df[output_df.Month.isin(test_months)]
