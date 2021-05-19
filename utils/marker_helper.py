import pandas as pd

from utils.string_helper import remove_underscore
from utils.timestamp_helper import convert_epoch_to_str


def epoch_marker(marker: dict) -> dict:
    df = pd.DataFrame.from_dict(marker, orient='index', columns=['marker'])

    df['epoch'] = df.index
    df['epoch'] = df['epoch'].apply(remove_underscore)

    markers = dict(df[['epoch', 'marker']].values[:])
    return markers


def readable_marker(marker: dict) -> dict:
    df = pd.DataFrame.from_dict(marker, orient='index', columns=['marker'])

    df['epoch'] = df.index
    df['epoch'] = df['epoch'].apply(remove_underscore)
    df['strdate'] = df['epoch'].apply(convert_epoch_to_str)

    markers = dict(df[['strdate', 'marker']].values[:])

    # print(markers)

    return markers


if __name__ == '__main__':
    m = {'_1598192406': '_1', '_1598192409': '_3', '_1598192413': '_4', '_1598192416': '_6'}
    d = epoch_marker(m)

    l = list(d.values())

    print(d)

