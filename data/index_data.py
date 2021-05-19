from data.index_data_record_keeping import IndexDataRecordKeeping
from columns import column_helper
from database.cosmos_factory import CosmosFactory


class IndexData:
    index_value = "index_value"
    variable = "variable"
    table_name = ".index_table"

    def __init__(self):
        cosmos = CosmosFactory.instance(keyspace_name=None)
        self.my_session = cosmos.get_session()
        self.keyspace = cosmos.get_keyspace_name()
        self.table_name = self.keyspace + self.table_name

        self.create_table()
        self.track = IndexDataRecordKeeping()

    def _format_row(self, rows):
        for r in rows:
            res = r[0]
            return res

    def _format_response_rows(self, rows) -> list:
        all_res = list()
        for r in rows:
            all_res.append(r[0])
        return all_res


    def _initial_insert(self, variable: str, idx: int):

        command = "INSERT INTO  " + self.table_name + " (" + self.variable + "," + self.index_value + ") VALUES ('" + variable + "'," + str(
            idx) + ")"
        ##print(command)
        self.my_session.execute(timeout=100, query=command)

    def get_all_variables(self) -> list:
        command = "SELECT " + self.variable + " FROM  " + self.table_name
        ##print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        return self._format_response_rows(rows)

    def get_all_indexes(self) -> list:
        command = "SELECT " + self.index_value + " FROM  " + self.table_name
        ##print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        return self._format_response_rows(rows)


    def variable_exists(self, variable: str) -> bool:
        command = "SELECT " + self.variable + " FROM  " + self.table_name + " WHERE " + self.variable + "='" + variable + "'"
        ##print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        r = self._format_row(rows)
        if r == None:
            return False
        else:
            return True

    def get_bitemporal_index(self, variable: str) -> int:
        if self.variable_exists(variable) == True:
            command = "SELECT " + self.index_value + " FROM  " + self.table_name + " WHERE " + self.variable + "='" + variable + "'"
            ##print(command)
            rows = self.my_session.execute(timeout=100, query=command)
            r = self._format_row(rows)
            return r
        else:
            return 0


    def get_variable_name(self, idx: int) -> str:
        command = "SELECT " + self.variable + " FROM  " + self.table_name + " WHERE " + self.index_value + "=" + str(idx) + " ALLOW FILTERING"
        #print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        r = self._format_row(rows)
        if r:
            return r
        else:
            return None



    def insert_data(self, variable: str) -> int:
        if self.variable_exists(variable) == False:
            idx = self.track.get_and_increment_value()
            self._initial_insert(variable, idx)
            return idx
        else:
            return self.get_bitemporal_index(variable)

    def get_column_names(self) -> list:

        command = "SELECT * FROM system_schema.columns WHERE keyspace_name = '" + self.keyspace + "' AND table_name = '" + self.table_name +"'"

        ##print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        names = list()
        for r in rows:
            names.append(r.column_name)
        return names;

    def column_exists(self, name: str) -> bool:
        names = self.get_column_names()
        if name in names:
            return True
        else:
            return False

    def insert_new_marker(self, variable: str, marker: str) -> str:
        ### need to create a new column here

        ## we need to start with the column label for that marker will be
        ## the label is currently the date

        col_name = column_helper.get_new_index_columm_name()
        if self.column_exists(col_name) == False:
            command = "ALTER TABLE " + self.table_name + " ADD " + col_name + " text"
            # print(command)
            self.my_session.execute(timeout=100, query=command)

        command = "UPDATE " + self.table_name + " SET " + col_name + " = '" + marker + "' WHERE " + self.variable + "='" + variable + "'"
        ##print(command)
        self.my_session.execute(timeout=100, query=command)

        return col_name

    def drop_table(self):
        self.my_session.execute(timeout=100, query='DROP TABLE IF EXISTS ' + self.table_name);

    def create_table(self, throughput: int=2000):
        self.my_session.execute(timeout=100, query=
        "CREATE TABLE IF NOT EXISTS " + self.table_name + " (" + self.variable + " text PRIMARY KEY, " + self.index_value + " int) WITH cosmosdb_provisioned_throughput=" + str(throughput));


    def chage_table_throughput(self, new_throughput: int):
        command = "ALTER TABLE " + self.table_name + " WITH cosmosdb_provisioned_throughput =" + str(new_throughput)
        # print(command)
        self.my_session.execute(timeout=100, query=command)


    def get_data_row(self, variable: str) -> dict:
        command = "SELECT * FROM  " + self.table_name + " WHERE " + self.variable + "='" + variable + "'"
        ##print(command)
        rows = self.my_session.execute(timeout=100, query=command)
        cols = rows.column_names
        for r in rows:
            res = list(r)

        result_dict = dict(zip(cols, res))

        n = dict((k, v) for k, v in result_dict.items() if v is not None)
        return n
