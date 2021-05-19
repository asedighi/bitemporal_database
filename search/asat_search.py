from data.bitemporal_data import BitemporalData
from data.index_data import IndexData
from database.cosmos_factory import CosmosFactory
from search.asof_search import AsOfSearch
from search.marker_search import MarkerSearch

from columns import column_helper
from utils import timestamp_helper
import datascience.pd_layer as ds


class AsAtSearch:

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()

        self.index = IndexData()

        self.rec = BitemporalData()

        self.marker = MarkerSearch()

        self.asof = AsOfSearch()

    def get_value_asat(self, variable: str, ts: str, asat_ts: str, hint: int):
        ## we get a dict of all the values first.  We are looking for asat
        asof_values = self.asof.get_value_asof(variable, ts, hint)

        # the keys should ALL be epoch time.  All values are string version of ints that represent sec from 1970

        k = list(asof_values[0].keys())
        k.remove('column')
        k.sort(reverse=True)

        ## short circuit test
        if int(asat_ts) >= k[0]:
            return (asof_values[0]).get(k[0])

        for i in k:
            if int(asat_ts) > i:
                return (asof_values[i]).get(k[0])

        return None


if __name__ == '__main__':
    import pandas as pd

    # d = {'variable': 'N_1', 'index_value': 3, '_1608565594': '_1', '_1608565651': '_3', '_1608565987': '_4', '_1608588072': '_12'}
    # d = [{1608565547: 'Value_3', 1608565559: 'Value_4', 1608565578: 'Value_5', 1608565582: 'Value_6', 'column': '_1'}]
    # df = ds.to_pd_dataframe(d, True)
    # print(df)

    # d =  {'1608565594': '_1', '1608565651': '_3', '1608565987': '_4', '1608588072': '_12'}
    # df = pd.DataFrame(d, index=["marker"])
    # df = df.transpose()
    # df = ds.to_pd_dataframe(d, True)
    # print(df)
    # exit()

    asof = AsOfSearch()

    asat = AsAtSearch()

    marker = MarkerSearch()

    index = IndexData()

    bt = BitemporalData()
    # for i in index.get_all_indexes():

    ## index value = 3
    variable_name = index.get_variable_name(3)
    print("Variable name to test: " + variable_name)
    print("All the data for varialble:\n")
    var_3_data = bt.get_data_row_as_df(3)

    print(var_3_data)

    m = marker.get_markers_as_df(variable_name, convert_time=True)
    print(m)
    m = marker.get_markers(variable_name)
    print(m)

    var_3_data = bt.get_data_row_as_df(3)

    print(var_3_data)

    m = marker.get_marked_data_by_index_as_df(3, "_1")
    print(m)

    m = marker.get_marked_data_range_by_index_as_df(3, "_1", "_3")
    print(m)

    new_col = bt.insert_data(3, "art_created_this", create_marker=True)

    print("A new col was created - pulling data again")

    m = bt.get_data_column_as_df(3, new_col)
    print(m)

    m = marker.get_markers_as_df(variable_name, convert_time=True)
    print(m)

    all = index.get_all_variables()

    for i in all:
        print("Variable: " + i)
        print("Latest: " + str(asof.get_latest(i)))
        print("As of 2 sec ago with 600 hint: " + str(asof.get_value_asof(i, timestamp_helper.get_n_sec_ago(2), 600)))
        print("As of 2 sec ago as at 60 min ago with 600 hint: " + str(
            asat.get_value_asat(i, timestamp_helper.get_n_sec_ago(2), timestamp_helper.get_n_min_ago(30), 600)))
