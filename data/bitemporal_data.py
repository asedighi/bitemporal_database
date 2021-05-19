import cassandra
from cassandra.cluster import Cluster, Session, ResultSet
import pandas as pd

from data.bitemporal_data_record_keeping import RecordKeepingData
from utils import timestamp_helper, string_helper

from database.cosmos_factory import CosmosFactory
from data.log_data import LogData
from data.index_data import IndexData
import datascience.pd_layer as ds


class BitemporalData:
    index_value = "index_value"
    table_name = ".value_table"

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()
        self.table_name = self.keyspace + self.table_name

        self.create_table()
        self.track = RecordKeepingData()
        self.log = LogData()
        self.index = IndexData()

    def _format_row(self, rows):
        for r in rows:
            res = r[0]
            return res

    def _format_a_row_values(self, rows) -> list:
        all_res = list()
        idx = 0
        for r in rows:
            for i, col in enumerate(r):

                if i == 0:
                    idx = r[i]
                else:
                    if col != None:

                        res_list = list()
                        for k in col.keys():
                            res_list.append(int(k))
                        partial_all = dict(zip(res_list, list(col.values())))
                        partial_all[self.index_value] = idx
                        partial_all["column"] = rows.column_names[i]
                        all_res.append(partial_all)

        return all_res

    def _format_column_values(self, rows) -> list:
        all_res = list()
        idx = 0
        for r in rows:
            for i, col in enumerate(r):
                if col != None:

                    res_list = list()
                    for k in col.keys():
                        res_list.append(int(k))
                    partial_all = dict(zip(res_list, list(col.values())))
                    partial_all["column"] = rows.column_names[i]
                    all_res.append(partial_all)

        return all_res

    def _initial_insert(self, idx: int):
        command = "INSERT INTO  " + self.table_name + " (" + self.index_value + ") VALUES (" + str(idx) + ")"
        # print(command)
        self.my_session.execute(timeout=100, query=command)

    def index_exists(self, idx: int) -> bool:
        command = "SELECT " + self.index_value + " FROM  " + self.table_name + " WHERE " + self.index_value + "=" + str(
            idx)
        # print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        r = self._format_row(rows)
        if r == None:
            return False
        else:
            return True

    def insert_data(self, idx: int, value: str, create_marker: bool = False) -> int:
        if self.index_exists(idx) == False:
            self._initial_insert(idx)

        col_name = self.create_column(idx)

        ts = timestamp_helper.get_timestamp()

        command = "UPDATE " + self.table_name + " SET " + col_name + " = " + col_name + " + {" + ts + ":'" + value + "'} WHERE index_value = " + str(
            idx)
        self.my_session.execute(timeout=100, query=command)

        self.log.insert_data(idx, ts, col_name)

        if create_marker:
            var = self.index.get_variable_name(idx=idx)
            self.index.insert_new_marker(variable=var, marker=col_name)

        return col_name

    def update_data(self, idx: int, value: str, col_name: str) -> int:
        if self.index_exists(idx) == False:
            self.insert_data(idx, value)
            return

        ts = timestamp_helper.get_timestamp()
        # print(ts)

        command = "UPDATE " + self.table_name + " SET " + col_name + " = " + col_name + " + {" + ts + ":'" + value + "'} WHERE index_value = " + str(
            idx)
        # print(command)
        self.my_session.execute(timeout=100, query=command)

        self.log.insert_data(idx, ts, col_name)

        return idx

    def create_column(self, idx: int) -> str:
        col_name = self.track.get_new_column_name(idx)

        if self.column_exists(col_name) == False:
            command = "ALTER TABLE " + self.table_name + " ADD " + col_name + " map<int, text>"
            # print(command)
            self.my_session.execute(timeout=100, query=command)

        return col_name

    def get_column_names(self) -> list:
        command = "SELECT * FROM system_schema.columns WHERE keyspace_name = '" + self.keyspace + "' AND table_name = 'value_table'"

        # print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        names = list()
        for r in rows:
            names.append(r.column_name)
        return names

    def column_exists(self, name: str) -> bool:
        names = self.get_column_names()

        if name in names:
            return True
        else:
            return False

    def drop_table(self):
        self.my_session.execute(timeout=100, query='DROP TABLE IF EXISTS ' + self.table_name);

    def chage_table_throughput(self, new_throughput: int):
        command = "ALTER TABLE " + self.table_name + " WITH cosmosdb_provisioned_throughput =" + str(new_throughput)
        # print(command)
        self.my_session.execute(timeout=100, query=command)

    def create_table(self, throughput: int = 2000):
        self.my_session.execute(timeout=100, query=
        "CREATE TABLE IF NOT EXISTS " + self.table_name + " (" + self.index_value + " int PRIMARY KEY) WITH cosmosdb_provisioned_throughput=" + str(
            throughput));

    def get_data_row(self, idx: int) -> list:
        command = "SELECT * FROM  " + self.table_name + " WHERE " + self.index_value + "=" + str(idx)
        # print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        return self._format_a_row_values(rows)

    def get_data_column(self, idx: int, col_name: str) -> list:
        if col_name == "_None":
            return None
        command = "SELECT " + col_name + " FROM  " + self.table_name + " WHERE " + self.index_value + "=" + str(idx)
        # print(command)
        rows: ResultSet = self.my_session.execute(timeout=100, query=command)
        return self._format_column_values(rows)
