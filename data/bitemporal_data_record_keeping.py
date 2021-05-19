
from cassandra.query import BatchStatement, SimpleStatement
import cassandra
from cassandra.cluster import Cluster, Session
from cassandra.policies import *
from database.cosmos_factory import CosmosFactory


class RecordKeepingData:

    index_value = "index_value"
    max_col = "max_column"
    table_name = ".record_keeping_table"


    def _format_row(self,rows) -> int:
        for r in rows:
            res = r[0]
            return res


    def _initial_insert(self, idx: int):

        command = "INSERT INTO  " + self.table_name + "  (" + self.index_value + "," + self.max_col + ") VALUES (" + str(idx) + ", 0)"
        #print(command)
        self.my_session.execute(timeout=100, query=command)

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()
        self.table_name = self.keyspace + self.table_name

        self.create_table()

    def get_latest_column(self, idx) -> str:
        return "_"+str(self._get_value(idx))


    def _get_value(self, idx) -> int:
        command = "SELECT " + self.max_col + " FROM " + self.table_name + " WHERE " + self.index_value + "=" + str(idx)
        #print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        return self._format_row(rows)

    def get_and_increment_value(self, idx) -> int:
        old_val = self._get_value(idx)

        if (old_val == None):
            self._initial_insert(idx)
            old_val = 1
        else:
            old_val= old_val + 1
        command = "UPDATE " + self.table_name + " SET " + self.max_col + " = " + str(old_val) + " WHERE " + self.index_value + "=" + str(idx)
        #print(command)

        self.my_session.execute(timeout=100, query=command)

        ### we shouldnt do this - but for debug purposes, it makes sense to read it again just in case
        return self._get_value(idx)
        #return old_val


    def get_new_column_name(self, idx):
        name = self.get_and_increment_value(idx)
        return "_"+str(name)


    def get_latest_column_name(self, idx):
        return "_"+str(self._get_value(idx))


    def drop_table(self):
        self.my_session.execute(timeout=100, query='DROP TABLE IF EXISTS ' + self.table_name);




    def create_table(self, throughput:int = 2000):
        self.my_session.execute(timeout=100, query=
            "CREATE TABLE IF NOT EXISTS " + self.table_name + " (index_value int PRIMARY KEY, max_column int) WITH cosmosdb_provisioned_throughput=" + str(throughput));

    def chage_table_throughput(self, new_throughput: int):
        command = "ALTER TABLE " + self.table_name + " WITH cosmosdb_provisioned_throughput =" + str(new_throughput)
        # print(command)
        self.my_session.execute(timeout=100, query=command)


