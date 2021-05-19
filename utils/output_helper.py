import pandas as pd
import datascience.pd_layer as ds


def PrintTable(rows):
    for r in rows:
        print(r)
        res = r[1]
        print(list(res.keys()))
        print(list(res.values()))

        res_list = list()
        for k in res.keys():
            res_list.append(int(k.timestamp() * 1000))
        a = dict()

        a = dict(zip(res_list, list(res.values())))

        print(a)


def format_rows(rows):
    for r in rows:
        print(r)
        res = r[1]
        print(list(r))


def PrettyPrint_Bitemporal_Values(d: dict):
    print(ds.to_pd_dataframe(d))
