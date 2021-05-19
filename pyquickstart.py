

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

from utils import output_helper, string_helper, timestamp_helper




ssl_opts = {
            'ca_certs': DEFAULT_CA_BUNDLE_PATH,
            'ssl_version': PROTOCOL_TLSv1_2,
            }

if 'certpath' in cfg.config:
    ssl_opts['ca_certs'] = cfg.config['certpath']

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider, ssl_options=ssl_opts
)
session = cluster.connect()

print ("\nCreating Keyspace")
session.execute('CREATE KEYSPACE IF NOT EXISTS bitemporaldb WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }');
print ("\nCreating Table")


session.execute('DROP TABLE IF EXISTS bitemporaldb.record_keeping_table');
session.execute('CREATE TABLE IF NOT EXISTS bitemporaldb.record_keeping_table (index_value int PRIMARY KEY, max_column int)');



session.execute('CREATE TABLE IF NOT EXISTS bitemporaldb.index_table (variable text PRIMARY KEY, index_value int)');
session.execute('DROP TABLE IF EXISTS bitemporaldb.value_table');



foo = str("_1")


foo2 = 'CREATE TABLE IF NOT EXISTS bitemporaldb.value_table (index_value int PRIMARY KEY,'+foo+' map<timestamp, text>)'
session.execute(foo2);

foo2 = "INSERT INTO  bitemporaldb.value_table  (index_value, "+foo+") VALUES (4, {"+timestamp_helper.get_timestamp()+":'" + string_helper.get_random_value() +"'})"
print(foo2)
session.execute(foo2)


foo2 = "UPDATE bitemporaldb.value_table SET "+foo+" = "+foo+" + {"+timestamp_helper.get_timestamp()+":'"+ string_helper.get_random_value() +"'} WHERE index_value = 4"
session.execute(foo2)


foo2 = "UPDATE bitemporaldb.value_table SET "+foo+" = "+foo+" + {"+timestamp_helper.get_timestamp()+":'"+ string_helper.get_random_value() +"'} WHERE index_value = 4"
session.execute(foo2)









print ("\nSelecting All")
rows = session.execute('SELECT * FROM bitemporaldb.value_table')
output_helper.PrintTable(rows)
