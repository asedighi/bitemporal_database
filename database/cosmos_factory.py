import random

from cassandra.auth import PlainTextAuthProvider
import database.config as cfg

import ssl
import cassandra
from cassandra.cluster import Cluster
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from utils import timestamp_helper

from requests.utils import DEFAULT_CA_BUNDLE_PATH


class CosmosFactory:

    _instance = None

    def __init__(self):
        raise RuntimeError('Call instance() instead')


    @classmethod
    def instance(cls, keyspace_name: str = None):
        if cls._instance is None:
            print('Creating new instance of Cosmos Factory')
            cls._instance = cls.__new__(cls)

            # Put any initialization here.
            ssl_opts = {
                'ca_certs': DEFAULT_CA_BUNDLE_PATH,
                'ssl_version': PROTOCOL_TLSv1_2,
            }

            if 'certpath' in cfg.config:
                ssl_opts['ca_certs'] = cfg.config['certpath']

            ssl_context = SSLContext(PROTOCOL_TLSv1_2)
            ssl_context.verify_mode = CERT_NONE

            auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])
            # cluster = Cluster([cfg.config['contactPoint']], port=int(cfg.config['port']), auth_provider=auth_provider,
            #                  ssl_options=ssl_opts, connect_timeout=600)

            cluster = Cluster([cfg.config['contactPoint']], port=int(cfg.config['port']), auth_provider=auth_provider,ssl_context=ssl_context, connect_timeout=600)
            cls._instance.session = cluster.connect()

            # self.create_keyspace(name='bitemporaldb', strategy_type='NetworkTopologyStrategy',  dc_name='datacenter',repl_factor=1)

            if keyspace_name == None and 'keyspace' in cfg.config and not cfg.config['keyspace']:

                cls._instance.name = "_" + timestamp_helper.get_timestamp()

                cls._instance.create_keyspace(name=cls._instance.name, strategy_type='NetworkTopologyStrategy', dc_name='datacenter', repl_factor=1)
            else:
                if keyspace_name == None and 'keyspace' in cfg.config and cfg.config['keyspace']:
                    cls._instance.name = cfg.config['keyspace']
                else:
                    cls._instance.name = keyspace_name


        return cls._instance

    def get_session(self):
        return self.session

    def get_keyspace_name(self):
        return self.name

    def create_keyspace(self, name, strategy_type, dc_name, repl_factor):

        print("Creating a keyspace called: " + name)
        self.session.execute(
            'CREATE KEYSPACE ' + name + ' WITH replication = {\'class\': \'' + strategy_type + '\', \'' + dc_name + '\' : \'' + str(
                repl_factor) + '\' }')

    def delete_keyspace(self):
        self.session.execute('DROP KEYSPACE IF EXISTS ' + self.name)



if __name__ == '__main__':

    cosmos = CosmosFactory.instance()


'''
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.verify_mode = CERT_NONE

    auth_provider = PlainTextAuthProvider(username=cfg.config['username'], password=cfg.config['password'])

    cluster = Cluster([cfg.config['contactPoint']], port=int(cfg.config['port']), auth_provider=auth_provider,
                      ssl_context=ssl_context, connect_timeout=600)
    session = cluster.connect()
'''