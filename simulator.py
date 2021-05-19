import random

import pprint

from datascience.asof_search_pd import AsOfSearchPandas
from datascience.bitemporal_pd import BitemporalPandas
from datascience.index_pd import IndexPandas
from datascience.marker_search_pd import MarkerSearchPandas
from utils import string_helper

from data.bitemporal_data import BitemporalData
from data.index_data import IndexData
from database.cosmos_factory import CosmosFactory
from data.index_data_record_keeping import IndexDataRecordKeeping
from data.bitemporal_data_record_keeping import RecordKeepingData
from search.marker_search import MarkerSearch
from search.asof_search import AsOfSearch
from database.config import config as cfg

from cleanup import clean_all

from utils import timestamp_helper

if __name__ == '__main__':

    ks_name = cfg['keyspace']
    db = CosmosFactory.instance(keyspace_name=ks_name)
    print(db.get_keyspace_name())
    db2 = CosmosFactory.instance(keyspace_name=ks_name)
    print(db.get_keyspace_name())

    if (id(db) == id(db2)):
        print("Same")



    index = IndexPandas()

    #print(index.get_all_variables())

    #index2 = IndexData()
    #print(index2.get_all_variables())

    rec = BitemporalPandas()
    #rec.update_data(idx=1, col_name="_23", value='Testing')


    #print(index.get_all_variables())


    if ks_name == None:
        #variable_1 = "NewVariable" + str(random.randint(100, 1000))
        #variable_2 = "OtherVariable" + str(random.randint(100, 1000))
        variable_1 = "N_" + string_helper.get_next_int()
        variable_2 = "O_" + string_helper.get_next_int()

        print("Does variable " + variable_1 + " exists? " + str(index.variable_exists(variable_1)))
        print("Does variable " + variable_2 + " exists? " + str(index.variable_exists(variable_2)))

        print("Inserting ...")
        index.insert_data(variable_1)
        index.insert_data(variable_2)

        print("Does variable " + variable_1 + " exists? " + str(index.variable_exists(variable_1)))
        print("Does variable " + variable_2 + " exists? " + str(index.variable_exists(variable_2)))

        for _ in range(2):

            print("Getting the indexes: ")

            idx1 = index.get_bitemporal_index(variable_1)
            idx2 = index.get_bitemporal_index(variable_2)

            print("Variable " + variable_1 + " has index: " + str(idx1))
            print("Variable " + variable_2 + " has index: " + str(idx2))

            sample_data_1 = string_helper.get_next_value()

            col_name_1_1 = rec.insert_data(idx1, sample_data_1)

            print("Column name: " + col_name_1_1)

            for _ in range(3):
                sample_data_1 = string_helper.get_next_value()

                rec.update_data(idx1, sample_data_1, col_name_1_1)

            m_col_name_1 = index.insert_new_marker(variable_1, col_name_1_1)

            print("Marker column name: " + m_col_name_1 + " marker value: " + col_name_1_1)

            sample_data_1 = string_helper.get_next_value()

            col_name_1_2 = rec.insert_data(idx1, sample_data_1)
            print("Column name: " + col_name_1_2)

            for _ in range(3):
                sample_data_1 = string_helper.get_next_value()

                rec.update_data(idx1, sample_data_1, col_name_1_2)

            sample_data_1 = string_helper.get_next_value()

            col_name_1_3 = rec.insert_data(idx1, sample_data_1)
            print("Column name: " + col_name_1_3)

            m_col_name_2 = index.insert_new_marker(variable_1, col_name_1_3)

            print("Marker column name: " + m_col_name_2 + " marker value: " + col_name_1_3)

            print("Index table: ")

            pprint.pprint(index.get_data_row(variable_1))
            # pprint.pprint(index.get_data_row(variable_2))

            print("Bitemporal table: ")

            pprint.pprint(rec.get_data_row(idx1))
            # pprint.pprint(rec.get_data_row(idx2))



    else:
        m_s = MarkerSearchPandas()
        asof = AsOfSearchPandas()

        all_vars = index.get_all_variables()

        print(all_vars)
        vars = list(all_vars['Variables'])

        for i in vars:
            idx: int = index.get_bitemporal_index(i)
            print("index for variable {} is {} ".format(i, idx))

            var: str = index.get_variable_name(idx)
            print("Variable name for index {} is {} ".format(idx, var))

            markers = m_s.get_markers(i)
            print(markers)

            j = list(markers['marker'])
            for i, k in enumerate(j):
                if i + 1 < len(j):
                    print(m_s.get_marked_data_range_by_index(idx, j[i], j[i + 1]))

                print()


        for i in vars:
            idx: int = index.get_bitemporal_index(i)
            print(rec.get_data_row(idx))
            #latest = asof.get_latest(i)
            latest = asof.get_latest(i)

            print("Latest value for {} is: ".format(i))
            print(latest)
            print(asof.get_value_asof(i, timestamp_helper.get_timestamp(), 36000))

        for i in vars:
            idx: int = index.get_bitemporal_index(i)

            sample_data_1 = string_helper.get_next_value()

            for _ in range(5):
                col_name_1_1 = rec.insert_data(idx, sample_data_1)

                print("Column name: " + col_name_1_1)

                for _ in range(3):
                    sample_data_1 = string_helper.get_next_value()

                    rec.update_data(idx, sample_data_1, col_name_1_1)

        for i in vars:
            latest = asof.get_latest(i)
            print("\nLatest value for {} is: ".format(i))
            print(latest)
            print(asof.get_value_asof(i, timestamp_helper.get_n_min_ago(2), hint=3600))

        # print(m_s.get_markers(variable_1))

        # print(m_s.get_markers_range(variable_1, timestamp_helper.get_n_sec_ago(10), timestamp_helper.get_tomorrow()))
