from data.bitemporal_data import BitemporalData
from data.index_data import IndexData
from database.cosmos_factory import CosmosFactory
from search.marker_search import MarkerSearch
from data.bitemporal_data_record_keeping import RecordKeepingData
from data.log_data import LogData
from utils import timestamp_helper

from columns import column_helper


class AsOfSearch:

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()

        self.index = IndexData()

        self.rec = BitemporalData()

        self.rec_keeper = RecordKeepingData()

        self.marker = MarkerSearch()

        self.log = LogData()

    def get_latest(self, variable: str) -> dict:
        if self.index.variable_exists(variable):
            idx = self.index.get_bitemporal_index(variable)
            if idx:
                #print(type(self))
                #if(type(self) == AsOfSearch):
                #    return self.get_latest_from_index(idx)
                #else:
                return AsOfSearch.get_latest_from_index(self, idx)

        else:
            raise "Variable " + variable + " does not exist"

    def get_latest_from_index(self, idx: int) -> dict:
        col = self.rec_keeper.get_latest_column_name(idx)
        return self.rec.get_data_column(idx, col)[-1]

    def get_value_asof(self, variable: str, ts: str, hint: int):
        if self.index.variable_exists(variable):
            idx = self.index.get_bitemporal_index(variable)
            # print("now " + timestamp_helper.convert_epoch_to_str(ts))

            before = timestamp_helper.get_now_and_then(ts, hint)
            # print("before " + timestamp_helper.convert_epoch_to_str(before))

            cols = self.log.get_column(idx, int(before), int(ts))
            retry = 0

            # if not cols:
            #    return cols
            # else:
            while not cols and retry < 5:
                hint = 2 * hint
                before = timestamp_helper.get_now_and_then(ts, hint)
                print("Looking back to: " + timestamp_helper.convert_epoch_to_str(before))
                cols = self.log.get_column(idx, int(before), int(ts))
                retry += 1

            if cols:
                ### looking for the 'latest' column
                latest_col = cols[-1]
                return self.rec.get_data_column(idx, latest_col)

            else:
                return None


if __name__ == '__main__':
    asof = AsOfSearch()
    index = IndexData()
    bt = BitemporalData()
    for i in index.get_all_indexes():
        print(bt.get_data_row(i))

    all = index.get_all_variables()

    for i in all:
        print(i)
        print(asof.get_latest(i))
        print(asof.get_value_asof(i, timestamp_helper.get_n_hrs_ago(2), 3600))
