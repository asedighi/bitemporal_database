
from utils import string_helper

from data.bitemporal_data import  BitemporalData
from data.index_data import IndexData
from database.cosmos_factory import CosmosFactory
from data.index_data_record_keeping import IndexDataRecordKeeping
from data.bitemporal_data_record_keeping import RecordKeepingData

from utils import timestamp_helper

def clean_all():

    db = CosmosFactory()

    index = IndexData()
    index.drop_table()

    rec = BitemporalData()
    rec.drop_table()

    keep = RecordKeepingData()
    keep.drop_table()


    keep = IndexDataRecordKeeping()
    keep.drop_table()





if __name__ == '__main__':
    clean_all()