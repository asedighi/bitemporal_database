import numpy as np
import re

from utils import output_helper, string_helper, timestamp_helper


def get_new_index_columm_name() -> str:
    return "_" + timestamp_helper.get_date()


def create_bitemporal_column_range(start: str, end: str) -> str:
    s = int(re.search(r'\d+', start).group())
    e = int(re.search(r'\d+', end).group())
    if s > e:
        raise Exception("End marker cannot be larger thant the start marker")

    cols = np.arange(s, e+1, 1).tolist()

    n_cols = ["_" + str(s) for s in cols]
    return ', '.join(n_cols)


if __name__ == '__main__':
    print(create_bitemporal_column_range("_123", "_144"))
