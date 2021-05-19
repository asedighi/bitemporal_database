import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

from database.cosmos_factory import CosmosFactory


class IndexDataRecordKeeping:
    index_name = "index_name"
    max_index_value = "max_index_value"
    table_name = ".index_record_keeping_table"
    index_name_variable = "idx_name"

    def _format_row(self, rows) -> int:
        for r in rows:
            res = r[0]
            return res

    def _initial_insert(self):

        command = "INSERT INTO " + self.table_name + " (" + self.index_name + "," + self.max_index_value + ") VALUES ('" + self.index_name_variable + "', 0)"

        # print(command)
        self.my_session.execute(timeout=100, query=command)

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()
        self.table_name = self.keyspace + self.table_name

        self.create_table()

    def _get_value(self) -> int:
        command = "SELECT " + self.max_index_value + " FROM " + self.table_name + " WHERE " + self.index_name + "='" + self.index_name_variable + "'"
        # print(command)

        rows = self.my_session.execute(timeout=100, query=command)
        return self._format_row(rows)

    def get_and_increment_value(self) -> int:
        old_val = self._get_value()

        if (old_val == None):
            self._initial_insert()
            old_val = 1
        else:
            old_val = old_val + 1
        command = "UPDATE " + self.table_name + " SET " + self.max_index_value + " = " + str(old_val) + " WHERE " + self.index_name + "='" + self.index_name_variable + "'"
        # print(command)

        self.my_session.execute(timeout=100, query=command)

        ### we shouldnt do this - but for debug purposes, it makes sense to read it again just in case
        return self._get_value()
        # return old_val

    def drop_table(self):
        self.my_session.execute(timeout=100, query="DROP TABLE IF EXISTS " + self.table_name);

    def create_table(self, throughput=2000):
        command = "CREATE TABLE IF NOT EXISTS " + self.table_name + " (" + self.index_name + " text PRIMARY KEY, " + self.max_index_value + " int) WITH cosmosdb_provisioned_throughput=" + str(
            throughput)
        # print(command)
        self.my_session.execute(timeout=100, query=command)

    def chage_table_throughput(self, new_throughput: int):
        command = "ALTER TABLE " + self.table_name + " WITH cosmosdb_provisioned_throughput =" + str(new_throughput)
        # print(command)
        self.my_session.execute(timeout=100, query=command)
