import pandas as pd

from utils import timestamp_helper
from utils import string_helper

def to_pd_dataframe(d, convert_time:bool = False, nan_to_zero: bool = True) -> pd.DataFrame:
    df = pd.DataFrame(d)
    df = df.dropna(how='all', axis=1)
    df = df.dropna(how='all', axis=0)
    if nan_to_zero:
        df = df.fillna(0)
    df = df.transpose()
    col = df[df.index == 'column']
    df = df[df.index != 'index_value']
    df = df[df.index != 'column']
    if len(col) > 0:
        df.columns = list(col.values)

    if convert_time:
        df = pd_convert_epoch(df)
    return df


def pd_convert_epoch(df: pd.DataFrame) -> pd.DataFrame:
    t = list()
    for i in df.index.values:
        t.append(timestamp_helper.convert_epoch_to_str(i))

    df2 = pd.DataFrame(t)
    df.insert(0, 'timestamp', df2.values)
    return df

if __name__ == '__main__':

    #d2 = ['O_2', 'NewVariable859', 'OtherVariable118', 'N_1']
    #d2 = {'variable': 'O_2', 'index_value': 4, '_1608601759': 'm1', '_1608588072': 'm2'}
    l = [{1608567343: 'Value_32', 'column': '_16'}]
    d2 = l[0]
    #df2 = to_pd_dataframe(d2)
    var = 'O_2'
    d2.pop('column')
    #d2['Variable'] = "foo"
    print(d2)
    df2 = pd.DataFrame(d2, index=[var])
#    df2.columns = [c]

    print(df2.transpose())

    l = [{1608567443: 'Value_33', 'column': '_16'}]
    d2 = l[0]
    # df2 = to_pd_dataframe(d2)
    var = 'O_3'
    d2.pop('column')
    # d2['Variable'] = "foo"
    print(d2)
    df3 = pd.DataFrame(d2, index=[var])
    #    df2.columns = [c]

    print(df3.transpose())

    df4 = df3.append(df2)

    print(df4.transpose())
    exit()

    l = list(df2.columns)
    l2 = list()

    l2.append('variable')
    l2.append('index_value')
    for line in l[2:]:
        print(line)
        l2.append(timestamp_helper.convert_epoch_to_str(string_helper.remove_underscore(line)))
        print(l2)


    df2.columns = l2

    print(df2)