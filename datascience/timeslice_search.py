from datascience.bitemporal_pd import BitemporalPandas
from data.index_data import IndexData
from database.cosmos_factory import CosmosFactory
from search.marker_search import MarkerSearch

from columns import column_helper
from datascience.pd_layer import pd_convert_epoch
import pandas as pd


class TimeSliceSearch:

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.index = IndexData()
        self.marker = MarkerSearch()
        self.bitemporal = BitemporalPandas()

    def get_all_marker_data(self, marker_point: str):
        all_vars = self.index.get_all_variables()
        df_final = pd.DataFrame()
        for i in all_vars:
            res = self.marker.get_marked_data(i, marker_point)
            if res:
                d = res[0]
                d.pop('column')
                df = pd.DataFrame(d, index=[i])
                df_final = df_final.append(df)


        mi = df_final.columns
        ind = pd.Index([e[0] for e in mi.tolist()])
        df_final.columns = ind
        df_final = df_final.transpose().fillna(0)


        return df_final


    def get_all_marker_to_marker(self, starting_marker: str, ending_marker: str):
        all_vars = self.index.get_all_variables()
        df_final = pd.DataFrame()
        for i in all_vars:
            res = self.marker.get_marked_data_range(i, marker_start=starting_marker, marker_end=ending_marker)
            for k in res:
                k.pop('column')
                df = pd.DataFrame(k, index=[i])
                df = df.transpose()
                df_final = df_final.append(df)

        mi = df_final.columns
        ind = pd.Index([e[0] for e in mi.tolist()])
        df_final.columns = ind

        df_final = df_final.fillna(0)

        return df_final

    def get_timeslice(self, variable: str, start_time: str, end_time: str) -> pd.DataFrame:
        idx = self.index.get_bitemporal_index(variable)
        if idx:
            df = self.bitemporal.get_data_row(idx)
            df.index=df.index.astype('int64')
            mi = df.columns
            ind = pd.Index([e[0] for e in mi.tolist()])
            df.columns = ind
            return df.loc[int(start_time):int(end_time)]
        return pd.DataFrame()

    def get_timebox(self, variable: str, start_time: str, end_time: str, starting_marker: str, ending_marker: str) -> pd.DataFrame:
        res = self.get_timeslice(variable, start_time, end_time)
        col = list(res.columns)
        res = res.loc[:, starting_marker:ending_marker]
        if res:
            return res



if __name__ == '__main__':
    t = TimeSliceSearch()
#    res = t.get_all_marker_to_marker("_1", "_2")

#    print(pd_convert_epoch(res))

    #res = t.get_timeslice("O_2", "1608567339", "1609421147")
    #print(res)

    res = t.get_timebox("O_2", "1608567339", "1609421147", "_1", "_4")
    print(res)
'''
    all = index.get_all_variables()

    for i in all:
        print(i)
        print(asof.get_latest(i))
        print(asof.get_value_asof(i, timestamp_helper.get_n_hrs_ago(2), 3600))
'''