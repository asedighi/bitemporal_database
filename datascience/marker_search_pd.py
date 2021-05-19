import pandas as pd
import datascience.pd_layer as ds
from search.marker_search import MarkerSearch


class MarkerSearchPandas(MarkerSearch):

    def __init__(self):
        super().__init__()


    def get_markers(self, variable: str, convert_time: bool = False) -> pd.DataFrame:
        try:
            res = super().get_markers(variable, readable_date=False)
            df = pd.DataFrame(res, index=["marker"])
            df = df.transpose()
            col = df[df.index == 'column']
            if len(col) > 0:
                df.columns = list(col.values)
            if convert_time:
                df = ds.pd_convert_epoch(df)
            return df
        except:
            raise "Variable " + variable + " does not exist"


    def get_marked_data(self, variable: str, marker: str) -> pd.DataFrame:
        res = super().get_marked_data(variable, marker)
        return ds.to_pd_dataframe(res)

    def get_marked_data_by_index(self, idx: int, marker: str) -> pd.DataFrame:
        res = super().get_marked_data_by_index(idx, marker)
        return ds.to_pd_dataframe(res)

    def get_marked_data_range(self, variable: str, marker_start: str, marker_end: str) -> pd.DataFrame:
        res = super().get_marked_data_range(variable, marker_start, marker_end)
        return ds.to_pd_dataframe(res)

    def get_marked_data_range_by_index(self, idx: int, marker_start: str, marker_end: str) -> pd.DataFrame:
        res = super().get_marked_data_range_by_index(idx, marker_start, marker_end)
        return ds.to_pd_dataframe(res)
