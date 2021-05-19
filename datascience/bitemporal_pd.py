import pandas as pd

import datascience.pd_layer as ds
from data.bitemporal_data import BitemporalData


class BitemporalPandas(BitemporalData):

    def __init__(self):
        super().__init__()

    def get_data_row(self, idx: int) -> pd.DataFrame:
        res = super().get_data_row(idx)
        return ds.to_pd_dataframe(res)

    def get_data_column(self, idx: int, col_name: str) -> pd.DataFrame:
        res = super().get_data_column(idx, col_name)
        return ds.to_pd_dataframe(res)
