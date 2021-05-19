from data.index_data_record_keeping import IndexDataRecordKeeping
from columns import column_helper
from database.cosmos_factory import CosmosFactory


class LogData:
    index_value = "index_value"
    timestamp = "ts"
    column = "col_name"
    table_name = ".log_table"

    _instance = None

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()
        self.table_name = self.keyspace + self.table_name

        self.create_table()

    '''
    def __new__(self, *args, **kwargs):
        if not self._instance:
            self._instance = super(LogData, self).__new__(
                self, *args, **kwargs)
        return self._instance
    '''

    def _format_row_values(self, rows) ->list:
        all_res = list()
        idx = 0
        for r in rows:
            all_res.append(r[0])

        return all_res

    def get_column(self, idx: int, start: int, end: int) -> list:
        # command = "SELECT * FROM  " + self.table_name + " WHERE " \

        command = "SELECT " + self.column + " FROM  " + self.table_name + " WHERE " \
                + str(self.timestamp) + " >= " + str(start) + " AND " \
                  + str(self.timestamp) + " < " + str(end) + " AND " \
                  + self.index_value + "=" + str(idx) + " ALLOW FILTERING"

        #print(command)
        row = self.my_session.execute(timeout=100, query=command)

        ###print(row)
        return self._format_row_values(row)

    def insert_data(self, idx: int, timestamp: int, column: str):
        command = "INSERT INTO  " + self.table_name + " (" + self.index_value + " , " + self.timestamp + " , " + self.column + ") VALUES (" + str(
            idx) + ", " + str(timestamp) + ", '" + column + "')"
        #print(command)
        self.my_session.execute(timeout=100, query=command)

    def drop_table(self):
        command = "DROP TABLE IF EXISTS " + self.table_name
        #print(command)

        self.my_session.execute(timeout=100, query=command);

    def create_table(self, throughput: int = 2000):
        command = "CREATE TABLE IF NOT EXISTS " + self.table_name + " (" + self.index_value + " int, " \
                  + self.timestamp + " int, " + self.column \
                  + " text, PRIMARY KEY ("+self.index_value+", "+self.timestamp+")) " \
                    "WITH cosmosdb_provisioned_throughput=" + str(throughput)
        #print(command)
        self.my_session.execute(timeout=100, query=command);

    def chage_table_throughput(self, new_throughput: int):
        command = "ALTER TABLE " + self.table_name + " WITH cosmosdb_provisioned_throughput =" + str(new_throughput)
        #print(command)
        self.my_session.execute(timeout=100, query=command)

if __name__ == '__main__':
    l = LogData()
    #l.drop_table()
    #l.create_table()
    #l.insert_data(1, 123120, "_2")
    #l.insert_data(1, 123130, "_3")
    #l.insert_data(1, 123140, "_4")
    #l.insert_data(1, 123150, "_5")
    #l.insert_data(1, 123160, "_6")
    #l.insert_data(1, 123170, "_7")

    print(l.get_column(1, 123120, 123150))
