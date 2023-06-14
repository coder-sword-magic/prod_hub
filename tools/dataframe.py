import pandas as pd


def convert_to_dt(df: pd.DataFrame,
                  params: list):
    for v in params:
        df[v] = pd.to_datetime(df[v], format='mixd')

    return df
