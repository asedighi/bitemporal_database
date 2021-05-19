from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable

import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH
from tables.table_helper import *
from database.cosmos_factory import *

def main():
    db = CosmosFactory()


    print("Table helper methods tester.")
    session = db.get_session()

    print('Connected to Cassandra on ', session)
    print ("\nCreating Keyspace")
    db.create_keyspace()


    print ("\nCreating Table")
    session.create_db_table(session, 'sujittest', 'msft')
    rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='sujittest'")
    for r in rows:
        table = r.table_name
    if (table != "msft"):
        print('Table creation failed.')
    print("\nDeleting Table")
    session.delete_db_table(session, 'sujittest', 'msft')
    print("\nDropping keyspace")
    session.execute('DROP KEYSPACE sujittest')

if __name__ == "__main__":
    main()
