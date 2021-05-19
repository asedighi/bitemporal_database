import pandas as pd

import datascience.pd_layer as ds
from data.index_data import IndexData
from utils import timestamp_helper, string_helper


class IndexPandas(IndexData):

    def __init__(self):
        #self.index = IndexData()

        super().__init__()

    def get_all_variables(self) -> pd.DataFrame:
        res = super().get_all_variables()
        idx = super().get_all_indexes()

        if (res):
            df = pd.DataFrame(res, columns=['Variables'], index=idx)
            df = df.sort_index()
            return df
        else:
            return None

    def get_all_indexes(self) -> pd.DataFrame:
        return super().get_all_variables()

    def get_column_names(self) -> pd.DataFrame:

        res = super().get_column_names()
        if (res):
            df = pd.DataFrame(res, columns=['Columns'])
            return df
        else:
            return None

    def get_data_row(self, variable: str,  convert_time: bool = False) -> pd.DataFrame:

        res = super().get_data_row(variable)

        df = pd.DataFrame(res, index=[0])

        if convert_time:

            l = list(df.columns)
            l2 = list()

            l2.append('variable')
            l2.append('index_value')
            for line in l[2:]:
                l2.append(timestamp_helper.convert_epoch_to_str(string_helper.remove_underscore(line)))

            df.columns = l2

        return df
