import pandas as pd

from datascience.bitemporal_pd import BitemporalPandas
from datascience.index_pd import IndexPandas
from search.asof_search import AsOfSearch


class AsOfSearchPandas(AsOfSearch):

    def __init__(self):
        super().__init__()

    def get_latest(self, variable: str) -> pd.DataFrame:
        res = super().get_latest(variable)

        c = res['column']
        res.pop('column')
        df = pd.DataFrame(res, index=list(res.keys()))
        df.columns = [c]
        return df

    def get_latest_from_index(self, idx: int) -> pd.DataFrame:
        res = super().get_latest_from_index(idx)
        if res:
            c = res[0]['column']
            res[0].pop('column')
            df = pd.DataFrame(res[0], index=list(res[0].keys()))
            df.columns = [c]
            return df
        else:
            return None

    def get_value_asof(self, variable: str, ts: str, hint: int) -> pd.DataFrame:
        res = super().get_value_asof(variable, ts, hint)
        if res:
            c = res[0]['column']
            res[0].pop('column')
            df = pd.DataFrame(res[0], index=list(res[0].keys()))
            df.columns = [c]
            return df
        else:
            return None

if __name__ == '__main__':
    asof = AsOfSearchPandas()
    all = ['O_2', 'NewVariable859', 'OtherVariable118', 'N_1']

    for i in all:
        print(i)
        print(asof.get_latest(i))
