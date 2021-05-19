from data.bitemporal_data import BitemporalData
from data.index_data import IndexData
from database.cosmos_factory import CosmosFactory
from utils.marker_helper import epoch_marker, readable_marker

from columns import column_helper


class MarkerSearch:

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()
        self.index = IndexData()
        self.rec = BitemporalData()

    def get_markers(self, variable: str, readable_date: bool = False) -> dict:
        if self.index.variable_exists(variable):
            row = self.index.get_data_row(variable)
            row.pop("index_value")
            row.pop("variable")
            if readable_date:
                return readable_marker(row)
            else:
                return epoch_marker(row)
        else:
            return None

    def get_markers_range(self, variable: str, starting_date: str, ending_date: str):
        all: dict = self.get_markers(variable, readable_date=True)
        s = int(starting_date)
        e = int(ending_date)


        for j in all.copy():
            new_j = int(j)
            if not (new_j > s and new_j < e):
                all.pop(j)

        return all

    def get_marked_data(self, variable: str, marker: str) -> list:

        if self.index.variable_exists(variable):
            idx = self.index.get_bitemporal_index(variable)
            return self.rec.get_data_column(idx, marker)

        else:
            raise "Variable " + variable + " does not exist"

    def get_marked_data_by_index(self, idx: int, marker: str):

        if (self.rec.index_exists(idx)):
            return self.rec.get_data_column(idx, marker)

        else:
            raise "Index " + idx + " does not exist"

    def get_marked_data_range(self, variable: str, marker_start: str, marker_end: str) -> list:

        if self.index.variable_exists(variable):
            idx = self.index.get_bitemporal_index(variable)
            cols = column_helper.create_bitemporal_column_range(marker_start, marker_end)
            return self.rec.get_data_column(idx, cols)

        else:
            raise "Variable " + variable + " does not exist"

    def get_marked_data_range_by_index(self, idx: int, marker_start: str, marker_end: str) -> list:

        if (self.rec.index_exists(idx)):
            cols = column_helper.create_bitemporal_column_range(marker_start, marker_end)
            return self.rec.get_data_column(idx, cols)

        else:
            raise "Index " + idx + " does not exist"
